"""Persistent candle + SuperTrend storage with SQLite (follows trade_store.py pattern)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import aiosqlite

from app.utils.logging import get_logger

log = get_logger(__name__)

_db: aiosqlite.Connection | None = None
_DATA_DIR = os.getenv("DATA_DIR", "data")
DB_PATH = Path(f"{_DATA_DIR}/candles.db")

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
    # rsi_10 kolonu yoksa ekle (migration)
    try:
        await db.execute("ALTER TABLE candles ADD COLUMN rsi_10 REAL")
    except Exception:
        pass  # Kolon zaten var
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


# ── RSI(10) Wilder's RMA hesaplama ──────────────────────


def calculate_rsi(closes: list[float], length: int = 10) -> list[float | None]:
    """Wilder's RMA ile RSI hesapla. TradingView ile birebir uyumlu.

    Returns: closes ile aynı uzunlukta liste, ilk `length` eleman None.
    """
    n = len(closes)
    rsi_values: list[float | None] = [None] * n

    if n < length + 1:
        return rsi_values

    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        delta = closes[i] - closes[i - 1]
        gains[i] = max(delta, 0.0)
        losses[i] = max(-delta, 0.0)

    # İlk avg: SMA
    avg_gain = sum(gains[1 : length + 1]) / length
    avg_loss = sum(losses[1 : length + 1]) / length

    if avg_loss == 0:
        rsi_values[length] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi_values[length] = 100.0 - (100.0 / (1.0 + rs))

    # Sonraki değerler: RMA (Wilder's smoothing)
    for i in range(length + 1, n):
        avg_gain = (avg_gain * (length - 1) + gains[i]) / length
        avg_loss = (avg_loss * (length - 1) + losses[i]) / length

        if avg_loss == 0:
            rsi_values[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_values[i] = round(100.0 - (100.0 / (1.0 + rs)), 4)

    return rsi_values


async def compute_and_store_rsi(
    symbol: str, interval: str, length: int = 10
) -> int:
    """DB'deki mumlardan RSI hesaplayıp rsi_10 kolonuna yaz. Return: güncellenen satır sayısı."""
    db = await get_candle_db()

    cursor = await db.execute(
        "SELECT open_time, close FROM candles WHERE symbol = ? AND interval = ? ORDER BY open_time ASC",
        (symbol, interval),
    )
    rows = await cursor.fetchall()

    if len(rows) < length + 1:
        return 0

    closes = [float(r["close"]) for r in rows]
    open_times = [int(r["open_time"]) for r in rows]

    rsi_values = calculate_rsi(closes, length)

    updated = 0
    for i in range(len(rows)):
        if rsi_values[i] is not None:
            await db.execute(
                "UPDATE candles SET rsi_10 = ? WHERE symbol = ? AND interval = ? AND open_time = ?",
                (rsi_values[i], symbol, interval, open_times[i]),
            )
            updated += 1

    await db.commit()
    return updated


async def get_recent_rsi(symbol: str, interval: str, count: int = 3) -> list[dict]:
    """Son N mumun RSI değerlerini döndür (en eskiden en yeniye)."""
    db = await get_candle_db()
    cursor = await db.execute(
        "SELECT open_time, date, close, rsi_10 FROM candles "
        "WHERE symbol = ? AND interval = ? AND rsi_10 IS NOT NULL "
        "ORDER BY open_time DESC LIMIT ?",
        (symbol, interval, count),
    )
    rows = await cursor.fetchall()
    return [dict(r) for r in reversed(rows)]
