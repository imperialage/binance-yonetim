"""Pine Monitor canli sinyal kayit deposu — Pine Script ile kiyas icin.

Frontend pine_monitor.html'in canli mum tick'lerinden urettigi sinyalleri
kalici olarak saklar (Railway /data volume — deploy ile silinmez).

Ayri DB dosyasi (pine_live.db), motor/Binance ile bagimsiz.
"""

from __future__ import annotations

import os
from pathlib import Path

import aiosqlite

from app.utils.logging import get_logger

log = get_logger(__name__)

_db: aiosqlite.Connection | None = None
_DATA_DIR = os.getenv("DATA_DIR", "data")
DB_PATH = Path(f"{_DATA_DIR}/pine_live.db")

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS pine_live_signals (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at    TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    interval      TEXT NOT NULL,
    direction     TEXT NOT NULL,
    entry_price   REAL NOT NULL,
    tp_price      REAL,
    sl_price      REAL,
    rsi_a         REAL,
    rsi_b         REAL,
    gap           INTEGER,
    a_time        INTEGER,
    bar_time      INTEGER,
    tick_price    REAL,
    rsi_len       INTEGER,
    long_thresh   REAL,
    short_thresh  REAL,
    max_gap       INTEGER,
    entry_buffer  REAL,
    tp_pct        REAL,
    sl_pct        REAL,
    UNIQUE(symbol, interval, bar_time, direction)
);
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_pls_symbol ON pine_live_signals(symbol);",
    "CREATE INDEX IF NOT EXISTS idx_pls_created ON pine_live_signals(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_pls_bar ON pine_live_signals(symbol, bar_time);",
]

# ── Sembol bazli parametreler (frontend pine_monitor'den gelir) ──
_CREATE_PARAMS_TABLE = """
CREATE TABLE IF NOT EXISTS pine_params (
    symbol        TEXT NOT NULL,
    interval      TEXT NOT NULL DEFAULT '5m',
    rsi_len       INTEGER NOT NULL DEFAULT 10,
    long_thresh   REAL NOT NULL DEFAULT 32,
    short_thresh  REAL NOT NULL DEFAULT 70,
    max_gap       INTEGER NOT NULL DEFAULT 12,
    entry_buffer  REAL NOT NULL DEFAULT 0.1,
    tp_pct        REAL NOT NULL DEFAULT 1.1,
    sl_pct        REAL NOT NULL DEFAULT 0.35,
    comm_pct      REAL NOT NULL DEFAULT 0.08,
    bars          INTEGER NOT NULL DEFAULT 500,
    active        INTEGER NOT NULL DEFAULT 1,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY(symbol, interval)
);
"""


async def get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _db = await aiosqlite.connect(str(DB_PATH))
        _db.row_factory = aiosqlite.Row
        await _db.execute("PRAGMA journal_mode=WAL;")
        await _db.execute("PRAGMA synchronous=NORMAL;")
    return _db


async def init_pine_live_db() -> None:
    db = await get_db()
    await db.execute(_CREATE_TABLE)
    await db.execute(_CREATE_PARAMS_TABLE)
    for idx in _CREATE_INDEXES:
        await db.execute(idx)
    await db.commit()
    await log.ainfo("pine_live_db_ready", path=str(DB_PATH))


# ── Parametre yonetimi ───────────────────────────────


async def upsert_params(symbol: str, interval: str, params: dict) -> None:
    """Sembol+interval parametrelerini kaydet/guncelle."""
    from datetime import datetime, timezone, timedelta
    tz_ist = timezone(timedelta(hours=3))
    now = datetime.now(tz_ist).strftime("%Y-%m-%d %H:%M:%S")
    db = await get_db()
    try:
        await db.execute(
            """INSERT INTO pine_params
               (symbol, interval, rsi_len, long_thresh, short_thresh, max_gap,
                entry_buffer, tp_pct, sl_pct, comm_pct, bars, active, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(symbol, interval) DO UPDATE SET
                 rsi_len=excluded.rsi_len,
                 long_thresh=excluded.long_thresh,
                 short_thresh=excluded.short_thresh,
                 max_gap=excluded.max_gap,
                 entry_buffer=excluded.entry_buffer,
                 tp_pct=excluded.tp_pct,
                 sl_pct=excluded.sl_pct,
                 comm_pct=excluded.comm_pct,
                 bars=excluded.bars,
                 active=excluded.active,
                 updated_at=excluded.updated_at""",
            (
                symbol.upper(),
                interval,
                int(params.get("rsi_len", 10)),
                float(params.get("long_thresh", 32)),
                float(params.get("short_thresh", 70)),
                int(params.get("max_gap", 12)),
                float(params.get("entry_buffer", 0.1)),
                float(params.get("tp_pct", 1.1)),
                float(params.get("sl_pct", 0.35)),
                float(params.get("comm_pct", 0.08)),
                int(params.get("bars", 500)),
                int(params.get("active", 1)),
                now,
            ),
        )
        await db.commit()
    except Exception as e:
        await log.aerror("pine_params_upsert_error", error=str(e))


async def get_params(symbol: str, interval: str = "5m") -> dict | None:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM pine_params WHERE symbol = ? AND interval = ?",
            (symbol.upper(), interval),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None
    except Exception as e:
        await log.aerror("pine_params_get_error", error=str(e))
        return None


async def get_all_active_params() -> list[dict]:
    """Aktif tum sembol+interval parametrelerini don."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM pine_params WHERE active = 1 ORDER BY symbol, interval"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        await log.aerror("pine_params_get_all_error", error=str(e))
        return []


async def insert_live_signal(data: dict) -> int | None:
    """Canli sinyali kaydet. UNIQUE constraint sayesinde duplike yazilmaz."""
    db = await get_db()
    try:
        cursor = await db.execute(
            """INSERT OR IGNORE INTO pine_live_signals
               (created_at, symbol, interval, direction, entry_price, tp_price, sl_price,
                rsi_a, rsi_b, gap, a_time, bar_time, tick_price,
                rsi_len, long_thresh, short_thresh, max_gap, entry_buffer, tp_pct, sl_pct)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                data.get("created_at"),
                data.get("symbol", "").upper(),
                data.get("interval", ""),
                data.get("direction", "").upper(),
                float(data.get("entry_price", 0)),
                float(data.get("tp_price", 0)) if data.get("tp_price") is not None else None,
                float(data.get("sl_price", 0)) if data.get("sl_price") is not None else None,
                float(data.get("rsi_a")) if data.get("rsi_a") is not None else None,
                float(data.get("rsi_b")) if data.get("rsi_b") is not None else None,
                int(data.get("gap")) if data.get("gap") is not None else None,
                int(data.get("a_time")) if data.get("a_time") is not None else None,
                int(data.get("bar_time")) if data.get("bar_time") is not None else None,
                float(data.get("tick_price")) if data.get("tick_price") is not None else None,
                int(data.get("rsi_len")) if data.get("rsi_len") is not None else None,
                float(data.get("long_thresh")) if data.get("long_thresh") is not None else None,
                float(data.get("short_thresh")) if data.get("short_thresh") is not None else None,
                int(data.get("max_gap")) if data.get("max_gap") is not None else None,
                float(data.get("entry_buffer")) if data.get("entry_buffer") is not None else None,
                float(data.get("tp_pct")) if data.get("tp_pct") is not None else None,
                float(data.get("sl_pct")) if data.get("sl_pct") is not None else None,
            ),
        )
        await db.commit()
        return cursor.lastrowid
    except Exception as e:
        await log.aerror("pine_live_insert_error", error=str(e))
        return None


async def query_live_signals(
    symbol: str | None = None,
    limit: int = 200,
) -> list[dict]:
    db = await get_db()
    try:
        if symbol:
            cursor = await db.execute(
                "SELECT * FROM pine_live_signals WHERE symbol = ? ORDER BY id DESC LIMIT ?",
                (symbol.upper(), limit),
            )
        else:
            cursor = await db.execute(
                "SELECT * FROM pine_live_signals ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        await log.aerror("pine_live_query_error", error=str(e))
        return []


async def close_pine_live_db() -> None:
    global _db
    if _db is not None:
        await _db.close()
        _db = None
