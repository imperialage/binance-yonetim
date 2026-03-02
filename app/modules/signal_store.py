"""Persistent signal logging with SQLite (dual-write alongside Redis)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import aiosqlite

from app.utils.logging import get_logger

log = get_logger(__name__)

_db: aiosqlite.Connection | None = None
DB_PATH = Path("data/signals.db")

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT UNIQUE NOT NULL,
    ts INTEGER NOT NULL,
    received_at INTEGER NOT NULL,
    indicator TEXT NOT NULL,
    symbol TEXT NOT NULL,
    tf TEXT NOT NULL,
    signal TEXT NOT NULL,
    strength REAL,
    price REAL,
    raw TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_signals_symbol_ts ON signals(symbol, ts);",
    "CREATE INDEX IF NOT EXISTS idx_signals_indicator ON signals(indicator);",
]


async def get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _db = await aiosqlite.connect(str(DB_PATH))
        _db.row_factory = aiosqlite.Row
    return _db


async def init_db() -> None:
    db = await get_db()
    await db.execute(_CREATE_TABLE)
    for idx_sql in _CREATE_INDEXES:
        await db.execute(idx_sql)
    await db.commit()
    await log.ainfo("signal_store_ready", path=str(DB_PATH))


async def close_db() -> None:
    global _db
    if _db is not None:
        await _db.close()
        _db = None
        await log.ainfo("signal_store_closed")


async def log_signal(event: Any) -> None:
    """Persist a NormalizedEvent to SQLite. Silently skips duplicates."""
    db = await get_db()
    # Support both Pydantic model and dict
    if hasattr(event, "model_dump"):
        d = event.model_dump()
    else:
        d = dict(event)

    raw_data = d.get("raw", {})
    if isinstance(raw_data, dict):
        raw_data.pop("secret", None)
        raw_json = json.dumps(raw_data)
    else:
        raw_json = str(raw_data)

    signal_val = d.get("signal", "")
    if hasattr(signal_val, "value"):
        signal_val = signal_val.value

    try:
        await db.execute(
            """INSERT OR IGNORE INTO signals
               (event_id, ts, received_at, indicator, symbol, tf, signal, strength, price, raw)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                d["event_id"],
                d["ts"],
                d["received_at"],
                d["indicator"],
                d["symbol"],
                d["tf"],
                signal_val,
                d.get("strength"),
                d.get("price"),
                raw_json,
            ),
        )
        await db.commit()
    except Exception as e:
        log.error("signal_store_write_error", event_id=d.get("event_id"), error=str(e))


async def query_signals(
    symbol: str,
    *,
    indicator: str | None = None,
    tf: str | None = None,
    signal: str | None = None,
    after: int | None = None,
    before: int | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Query persisted signals from SQLite with optional filters."""
    db = await get_db()
    conditions = ["symbol = ?"]
    params: list[Any] = [symbol.strip().upper()]

    if indicator:
        conditions.append("indicator = ?")
        params.append(indicator)
    if tf:
        conditions.append("tf = ?")
        params.append(tf.lower())
    if signal:
        conditions.append("signal = ?")
        params.append(signal.upper())
    if after is not None:
        conditions.append("ts >= ?")
        params.append(after)
    if before is not None:
        conditions.append("ts <= ?")
        params.append(before)

    where = " AND ".join(conditions)
    params.append(limit)

    cursor = await db.execute(
        f"SELECT * FROM signals WHERE {where} ORDER BY ts DESC LIMIT ?",  # noqa: S608
        params,
    )
    rows = await cursor.fetchall()

    results = []
    for row in rows:
        d = dict(row)
        # Parse raw JSON back to dict
        if d.get("raw"):
            try:
                d["raw"] = json.loads(d["raw"])
            except (json.JSONDecodeError, TypeError):
                pass
        results.append(d)

    return results
