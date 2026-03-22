"""Persistent candle + SuperTrend storage with SQLite (follows trade_store.py pattern)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import aiosqlite

from app.utils.logging import get_logger

log = get_logger(__name__)

_db: aiosqlite.Connection | None = None
DB_PATH = Path("data/candles.db")

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS candles (
    symbol      TEXT NOT NULL,
    interval    TEXT NOT NULL,
    open_time   INTEGER NOT NULL,
    date        TEXT NOT NULL,
    open        REAL NOT NULL,
    high        REAL NOT NULL,
    low         REAL NOT NULL,
    close       REAL NOT NULL,
    volume      REAL NOT NULL,
    supertrend  REAL,
    direction   TEXT,
    cluster     TEXT,
    signal      TEXT,
    PRIMARY KEY (symbol, interval, open_time)
);
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_candles_sym_iv_time ON candles(symbol, interval, open_time);",
    "CREATE INDEX IF NOT EXISTS idx_candles_signal ON candles(signal) WHERE signal != '';",
]


async def get_candle_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _db = await aiosqlite.connect(str(DB_PATH))
        _db.row_factory = aiosqlite.Row
        await _db.execute("PRAGMA journal_mode=WAL;")
        await _db.execute("PRAGMA synchronous=NORMAL;")
    return _db


async def init_candle_db() -> None:
    db = await get_candle_db()
    await db.execute(_CREATE_TABLE)
    for idx_sql in _CREATE_INDEXES:
        await db.execute(idx_sql)
    await db.commit()
    await log.ainfo("candle_store_ready", path=str(DB_PATH))


async def close_candle_db() -> None:
    global _db
    if _db is not None:
        await _db.close()
        _db = None
        await log.ainfo("candle_store_closed")


async def upsert_candles(rows: list[dict], symbol: str, interval: str) -> int:
    """Mum verilerini veritabanına yaz (varsa güncelle, yoksa ekle)."""
    if not rows:
        return 0

    db = await get_candle_db()
    count = 0
    for r in rows:
        await db.execute(
            """INSERT INTO candles
               (symbol, interval, open_time, date, open, high, low, close, volume,
                supertrend, direction, cluster, signal)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(symbol, interval, open_time)
               DO UPDATE SET
                 date=excluded.date, open=excluded.open, high=excluded.high,
                 low=excluded.low, close=excluded.close, volume=excluded.volume,
                 supertrend=excluded.supertrend, direction=excluded.direction,
                 cluster=excluded.cluster, signal=excluded.signal""",
            (
                symbol,
                interval,
                r["open_time"],
                r["date"],
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["volume"],
                r.get("supertrend"),
                r.get("direction"),
                r.get("cluster"),
                r.get("signal", ""),
            ),
        )
        count += 1
    await db.commit()
    return count


async def get_last_open_time(symbol: str, interval: str) -> int | None:
    """Veritabanındaki en son mum zamanını döndür (ms)."""
    db = await get_candle_db()
    cursor = await db.execute(
        "SELECT MAX(open_time) FROM candles WHERE symbol = ? AND interval = ?",
        (symbol, interval),
    )
    row = await cursor.fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None


async def get_first_open_time(symbol: str, interval: str) -> int | None:
    """Veritabanındaki en eski mum zamanını döndür (ms)."""
    db = await get_candle_db()
    cursor = await db.execute(
        "SELECT MIN(open_time) FROM candles WHERE symbol = ? AND interval = ?",
        (symbol, interval),
    )
    row = await cursor.fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None


async def query_candles(
    symbol: str,
    interval: str,
    *,
    limit: int = 200,
    signals_only: bool = False,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict[str, Any]]:
    """Son N mumu veritabanından oku. Opsiyonel tarih filtresi."""
    db = await get_candle_db()
    where = "symbol = ? AND interval = ?"
    params: list[Any] = [symbol, interval]

    if signals_only:
        where += " AND signal != ''"

    if date_from:
        where += " AND date >= ?"
        params.append(date_from)

    if date_to:
        where += " AND date <= ?"
        params.append(date_to + " 23:59:59")

    cursor = await db.execute(
        f"SELECT * FROM candles WHERE {where} ORDER BY open_time DESC LIMIT ?",  # noqa: S608
        [*params, limit],
    )
    rows = await cursor.fetchall()
    return [dict(r) for r in reversed(rows)]


async def get_raw_klines(
    symbol: str,
    interval: str,
    limit: int = 2100,
) -> list[list]:
    """DB'den Binance kline formatında ham veri döndür (SuperTrend warmup için)."""
    db = await get_candle_db()
    cursor = await db.execute(
        "SELECT open_time, open, high, low, close, volume FROM candles "
        "WHERE symbol = ? AND interval = ? ORDER BY open_time DESC LIMIT ?",
        [symbol, interval, limit],
    )
    rows = await cursor.fetchall()
    # Binance kline format: [open_time, open, high, low, close, volume, ...]
    result = []
    for r in reversed(rows):
        result.append([
            r[0],       # open_time (ms)
            str(r[1]),  # open
            str(r[2]),  # high
            str(r[3]),  # low
            str(r[4]),  # close
            str(r[5]),  # volume
            0, "0", "0", "0", "0", 0,  # padding to match Binance format
        ])
    return result


async def get_candle_stats(symbol: str, interval: str) -> dict[str, Any]:
    """Mum verisi istatistikleri."""
    db = await get_candle_db()

    cursor = await db.execute(
        "SELECT COUNT(*) FROM candles WHERE symbol = ? AND interval = ?",
        (symbol, interval),
    )
    total = (await cursor.fetchone())[0]

    if total == 0:
        return {"exists": False, "rows": 0}

    cursor = await db.execute(
        """SELECT
             MIN(date) as first_date,
             MAX(date) as last_date,
             SUM(CASE WHEN signal = 'BUY' THEN 1 ELSE 0 END) as buy_signals,
             SUM(CASE WHEN signal = 'SELL' THEN 1 ELSE 0 END) as sell_signals,
             SUM(CASE WHEN signal != '' THEN 1 ELSE 0 END) as total_signals
           FROM candles WHERE symbol = ? AND interval = ?""",
        (symbol, interval),
    )
    row = await cursor.fetchone()

    return {
        "exists": True,
        "rows": total,
        "first_date": row["first_date"],
        "last_date": row["last_date"],
        "total_signals": row["total_signals"],
        "buy_signals": row["buy_signals"],
        "sell_signals": row["sell_signals"],
    }
