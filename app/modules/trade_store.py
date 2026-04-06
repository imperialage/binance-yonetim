"""Persistent trade logging with SQLite (follows signal_store.py pattern)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import aiosqlite

from app.utils.logging import get_logger

log = get_logger(__name__)

_db: aiosqlite.Connection | None = None
_DATA_DIR = os.getenv("DATA_DIR", "data")
DB_PATH = Path(f"{_DATA_DIR}/trades.db")

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,
    quantity        REAL,
    entry_price     REAL,
    stop_price      REAL,
    order_id        TEXT,
    stop_order_id   TEXT,
    status          TEXT NOT NULL,
    reason          TEXT,
    closed_previous INTEGER DEFAULT 0,
    balance_used    REAL,
    duration_ms     INTEGER,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

_CREATE_TRADE_CLOSURES = """
CREATE TABLE IF NOT EXISTS trade_closures (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id             TEXT,
    signal_id            INTEGER,
    symbol               TEXT NOT NULL,
    direction            TEXT NOT NULL,
    entry_price          REAL NOT NULL,
    entry_time           INTEGER NOT NULL,
    exit_price           REAL NOT NULL,
    exit_time            INTEGER NOT NULL,
    exit_reason          TEXT NOT NULL,
    qty                  REAL NOT NULL,
    pnl_usdt             REAL NOT NULL DEFAULT 0,
    pnl_pct              REAL NOT NULL DEFAULT 0,
    commission           REAL NOT NULL DEFAULT 0,
    hold_duration_seconds INTEGER NOT NULL DEFAULT 0,
    tp_price             REAL,
    sl_price             REAL,
    created_at           TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

_CREATE_POST_EXIT_CANDLES = """
CREATE TABLE IF NOT EXISTS post_exit_candles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    closure_id      INTEGER NOT NULL,
    candle_index    INTEGER NOT NULL,
    open_time       INTEGER NOT NULL,
    high            REAL NOT NULL,
    low             REAL NOT NULL,
    close           REAL NOT NULL,
    UNIQUE(closure_id, candle_index)
);
"""

_CREATE_DAILY_METRICS = """
CREATE TABLE IF NOT EXISTS daily_metrics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT NOT NULL UNIQUE,
    total_trades    INTEGER NOT NULL DEFAULT 0,
    wins            INTEGER NOT NULL DEFAULT 0,
    losses          INTEGER NOT NULL DEFAULT 0,
    win_rate        REAL NOT NULL DEFAULT 0,
    total_pnl_usdt  REAL NOT NULL DEFAULT 0,
    total_pnl_pct   REAL NOT NULL DEFAULT 0,
    avg_hold_seconds INTEGER NOT NULL DEFAULT 0,
    tp_count        INTEGER NOT NULL DEFAULT 0,
    sl_count        INTEGER NOT NULL DEFAULT 0,
    manual_count    INTEGER NOT NULL DEFAULT 0,
    best_pnl_pct    REAL,
    worst_pnl_pct   REAL,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON trades(symbol, ts);",
    "CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);",
    "CREATE INDEX IF NOT EXISTS idx_tc_symbol_exit ON trade_closures(symbol, exit_time);",
    "CREATE INDEX IF NOT EXISTS idx_tc_exit_reason ON trade_closures(exit_reason);",
    "CREATE INDEX IF NOT EXISTS idx_tc_direction ON trade_closures(direction);",
    "CREATE INDEX IF NOT EXISTS idx_pec_closure ON post_exit_candles(closure_id);",
    "CREATE INDEX IF NOT EXISTS idx_dm_date ON daily_metrics(date);",
]

_MIGRATIONS = [
    "ALTER TABLE trades ADD COLUMN signal_id INTEGER",
]


async def get_trade_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _db = await aiosqlite.connect(str(DB_PATH))
        _db.row_factory = aiosqlite.Row
        await _db.execute("PRAGMA journal_mode=WAL;")
        await _db.execute("PRAGMA synchronous=NORMAL;")
    return _db


async def init_trade_db() -> None:
    db = await get_trade_db()
    await db.execute(_CREATE_TABLE)
    await db.execute(_CREATE_TRADE_CLOSURES)
    await db.execute(_CREATE_POST_EXIT_CANDLES)
    await db.execute(_CREATE_DAILY_METRICS)
    for idx_sql in _CREATE_INDEXES:
        await db.execute(idx_sql)
    for mig in _MIGRATIONS:
        try:
            await db.execute(mig)
        except Exception:
            pass  # kolon zaten var
    await db.commit()
    await log.ainfo("trade_store_ready", path=str(DB_PATH))


async def close_trade_db() -> None:
    global _db
    if _db is not None:
        await _db.close()
        _db = None
        await log.ainfo("trade_store_closed")


async def log_trade(
    *,
    event_id: str,
    ts: int,
    symbol: str,
    side: str,
    quantity: float | None = None,
    entry_price: float | None = None,
    stop_price: float | None = None,
    order_id: str | None = None,
    stop_order_id: str | None = None,
    status: str,
    reason: str | None = None,
    closed_previous: bool = False,
    balance_used: float | None = None,
    duration_ms: int | None = None,
    signal_id: int | None = None,
) -> None:
    """Persist a trade record to SQLite."""
    db = await get_trade_db()
    try:
        await db.execute(
            """INSERT INTO trades
               (event_id, ts, symbol, side, quantity, entry_price, stop_price,
                order_id, stop_order_id, status, reason, closed_previous,
                balance_used, duration_ms, signal_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                event_id,
                ts,
                symbol,
                side,
                quantity,
                entry_price,
                stop_price,
                order_id,
                stop_order_id,
                status,
                reason,
                1 if closed_previous else 0,
                balance_used,
                duration_ms,
                signal_id,
            ),
        )
        await db.commit()
    except Exception as e:
        log.error("trade_store_write_error", event_id=event_id, error=str(e))


async def query_trades(
    symbol: str | None = None,
    *,
    side: str | None = None,
    status: str | None = None,
    after: int | None = None,
    before: int | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Query persisted trades from SQLite with optional filters."""
    db = await get_trade_db()
    conditions: list[str] = []
    params: list[Any] = []

    if symbol:
        conditions.append("symbol = ?")
        params.append(symbol.strip().upper())
    if side:
        conditions.append("side = ?")
        params.append(side.upper())
    if status:
        conditions.append("status = ?")
        params.append(status.upper())
    if after is not None:
        conditions.append("ts >= ?")
        params.append(after)
    if before is not None:
        conditions.append("ts <= ?")
        params.append(before)

    where = " AND ".join(conditions) if conditions else "1=1"
    params.append(limit)

    cursor = await db.execute(
        f"SELECT * FROM trades WHERE {where} ORDER BY ts DESC LIMIT ?",  # noqa: S608
        params,
    )
    rows = await cursor.fetchall()
    return [dict(row) for row in rows]


# ── trade_closures ──────────────────────────────────────


async def log_closure(
    *,
    event_id: str | None = None,
    signal_id: int | None = None,
    symbol: str,
    direction: str,
    entry_price: float,
    entry_time: int,
    exit_price: float,
    exit_time: int,
    exit_reason: str,
    qty: float,
    pnl_usdt: float = 0,
    pnl_pct: float = 0,
    commission: float = 0,
    hold_duration_seconds: int = 0,
    tp_price: float | None = None,
    sl_price: float | None = None,
) -> int:
    """Persist a trade closure record. Returns row id."""
    db = await get_trade_db()
    try:
        cursor = await db.execute(
            """INSERT INTO trade_closures
               (event_id, signal_id, symbol, direction, entry_price, entry_time,
                exit_price, exit_time, exit_reason, qty, pnl_usdt, pnl_pct,
                commission, hold_duration_seconds, tp_price, sl_price)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                event_id, signal_id, symbol, direction, entry_price, entry_time,
                exit_price, exit_time, exit_reason, qty, pnl_usdt, pnl_pct,
                commission, hold_duration_seconds, tp_price, sl_price,
            ),
        )
        await db.commit()
        return cursor.lastrowid or 0
    except Exception as e:
        log.error("closure_write_error", symbol=symbol, error=str(e))
        return 0


async def query_closures(
    symbol: str | None = None,
    *,
    direction: str | None = None,
    exit_reason: str | None = None,
    after: int | None = None,
    before: int | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Query trade closures with filters."""
    db = await get_trade_db()
    conditions: list[str] = []
    params: list[Any] = []
    if symbol:
        conditions.append("symbol = ?")
        params.append(symbol.upper())
    if direction:
        conditions.append("direction = ?")
        params.append(direction.upper())
    if exit_reason:
        conditions.append("exit_reason = ?")
        params.append(exit_reason.upper())
    if after is not None:
        conditions.append("exit_time >= ?")
        params.append(after)
    if before is not None:
        conditions.append("exit_time <= ?")
        params.append(before)
    where = " AND ".join(conditions) if conditions else "1=1"
    params.append(limit)
    cursor = await db.execute(
        f"SELECT * FROM trade_closures WHERE {where} ORDER BY exit_time DESC LIMIT ?",  # noqa: S608
        params,
    )
    rows = await cursor.fetchall()
    return [dict(row) for row in rows]


# ── post_exit_candles ───────────────────────────────────


async def log_post_exit_candles(closure_id: int, candles: list[dict]) -> None:
    """Store candle data after a trade exit (for SL analysis)."""
    db = await get_trade_db()
    for c in candles:
        try:
            await db.execute(
                """INSERT OR IGNORE INTO post_exit_candles
                   (closure_id, candle_index, open_time, high, low, close)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (closure_id, c["index"], c["open_time"], c["high"], c["low"], c["close"]),
            )
        except Exception:
            pass
    await db.commit()


async def get_post_exit_candles(closure_id: int) -> list[dict]:
    """Get post-exit candles for a closure."""
    db = await get_trade_db()
    cursor = await db.execute(
        "SELECT * FROM post_exit_candles WHERE closure_id = ? ORDER BY candle_index",
        (closure_id,),
    )
    rows = await cursor.fetchall()
    return [dict(r) for r in rows]


# ── daily_metrics ───────────────────────────────────────


async def upsert_daily_metrics(date_str: str, metrics: dict) -> None:
    """Insert or update daily metrics."""
    db = await get_trade_db()
    await db.execute(
        """INSERT INTO daily_metrics
           (date, total_trades, wins, losses, win_rate, total_pnl_usdt, total_pnl_pct,
            avg_hold_seconds, tp_count, sl_count, manual_count, best_pnl_pct, worst_pnl_pct)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(date) DO UPDATE SET
            total_trades=excluded.total_trades, wins=excluded.wins, losses=excluded.losses,
            win_rate=excluded.win_rate, total_pnl_usdt=excluded.total_pnl_usdt,
            total_pnl_pct=excluded.total_pnl_pct, avg_hold_seconds=excluded.avg_hold_seconds,
            tp_count=excluded.tp_count, sl_count=excluded.sl_count, manual_count=excluded.manual_count,
            best_pnl_pct=excluded.best_pnl_pct, worst_pnl_pct=excluded.worst_pnl_pct""",
        (
            date_str, metrics.get("total_trades", 0), metrics.get("wins", 0),
            metrics.get("losses", 0), metrics.get("win_rate", 0),
            metrics.get("total_pnl_usdt", 0), metrics.get("total_pnl_pct", 0),
            metrics.get("avg_hold_seconds", 0), metrics.get("tp_count", 0),
            metrics.get("sl_count", 0), metrics.get("manual_count", 0),
            metrics.get("best_pnl_pct"), metrics.get("worst_pnl_pct"),
        ),
    )
    await db.commit()


async def query_daily_metrics(days: int = 30) -> list[dict]:
    """Get daily metrics for last N days."""
    db = await get_trade_db()
    cursor = await db.execute(
        "SELECT * FROM daily_metrics ORDER BY date DESC LIMIT ?", (days,)
    )
    rows = await cursor.fetchall()
    return [dict(r) for r in rows]
