"""Persistent trade logging with SQLite (follows signal_store.py pattern)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import aiosqlite

from app.utils.logging import get_logger

log = get_logger(__name__)

_db: aiosqlite.Connection | None = None
DB_PATH = Path("data/trades.db")

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

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON trades(symbol, ts);",
    "CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);",
]


async def get_trade_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _db = await aiosqlite.connect(str(DB_PATH))
        _db.row_factory = aiosqlite.Row
    return _db


async def init_trade_db() -> None:
    db = await get_trade_db()
    await db.execute(_CREATE_TABLE)
    for idx_sql in _CREATE_INDEXES:
        await db.execute(idx_sql)
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
) -> None:
    """Persist a trade record to SQLite."""
    db = await get_trade_db()
    try:
        await db.execute(
            """INSERT INTO trades
               (event_id, ts, symbol, side, quantity, entry_price, stop_price,
                order_id, stop_order_id, status, reason, closed_previous,
                balance_used, duration_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
