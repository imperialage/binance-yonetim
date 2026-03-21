"""SuperTrend signal logging — signal_log + signal_stats tables (aiosqlite)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import aiosqlite

from app.utils.logging import get_logger

log = get_logger(__name__)

_db: aiosqlite.Connection | None = None
DB_PATH = Path("data/st_signals.db")

# ── Table definitions ─────────────────────────────────────

_CREATE_SIGNAL_LOG = """
CREATE TABLE IF NOT EXISTS signal_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    datetime     TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    direction    TEXT NOT NULL,
    band         TEXT NOT NULL,
    price        REAL NOT NULL,
    vol_ratio    REAL,
    entered      INTEGER NOT NULL DEFAULT 0,
    skip_filter  TEXT,
    skip_reason  TEXT,
    outcome_pct  REAL,
    hit_target   INTEGER
);
"""

_CREATE_SIGNAL_STATS = """
CREATE TABLE IF NOT EXISTS signal_stats (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    direction    TEXT NOT NULL,
    band         TEXT NOT NULL,
    hour_bucket  INTEGER NOT NULL,
    success_rate REAL NOT NULL DEFAULT 0.0,
    sample_count INTEGER NOT NULL DEFAULT 0,
    updated_at   TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(direction, band, hour_bucket)
);
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_sl_symbol_dt ON signal_log(symbol, datetime);",
    "CREATE INDEX IF NOT EXISTS idx_sl_direction ON signal_log(direction);",
    "CREATE INDEX IF NOT EXISTS idx_sl_entered ON signal_log(entered);",
    "CREATE INDEX IF NOT EXISTS idx_ss_lookup ON signal_stats(direction, band, hour_bucket);",
]


# ── DB lifecycle ──────────────────────────────────────────

async def get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _db = await aiosqlite.connect(str(DB_PATH))
        _db.row_factory = aiosqlite.Row
        await _db.execute("PRAGMA journal_mode=WAL;")
        await _db.execute("PRAGMA synchronous=NORMAL;")
    return _db


async def init_st_signal_db() -> None:
    db = await get_db()
    await db.execute(_CREATE_SIGNAL_LOG)
    await db.execute(_CREATE_SIGNAL_STATS)
    for idx_sql in _CREATE_INDEXES:
        await db.execute(idx_sql)
    await db.commit()
    await log.ainfo("st_signal_db_ready", path=str(DB_PATH))


async def close_st_signal_db() -> None:
    global _db
    if _db is not None:
        await _db.close()
        _db = None
        await log.ainfo("st_signal_db_closed")


# ── signal_log operations ─────────────────────────────────

async def log_st_signal(
    *,
    dt: str,
    symbol: str,
    direction: str,
    band: str,
    price: float,
    vol_ratio: float | None = None,
    entered: bool = False,
    skip_filter: str | None = None,
    skip_reason: str | None = None,
) -> int:
    """Insert a signal record. Returns the row id."""
    db = await get_db()
    try:
        cursor = await db.execute(
            """INSERT INTO signal_log
               (datetime, symbol, direction, band, price, vol_ratio,
                entered, skip_filter, skip_reason)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                dt,
                symbol.upper(),
                direction.upper(),
                band.upper(),
                price,
                vol_ratio,
                1 if entered else 0,
                skip_filter,
                skip_reason,
            ),
        )
        await db.commit()
        return cursor.lastrowid or 0
    except Exception as e:
        log.error("st_signal_log_write_error", error=str(e))
        return 0


async def update_outcome(row_id: int, outcome_pct: float, hit_target: bool) -> None:
    """Update outcome for a logged signal (called later by stats updater)."""
    db = await get_db()
    try:
        await db.execute(
            "UPDATE signal_log SET outcome_pct = ?, hit_target = ? WHERE id = ?",
            (outcome_pct, 1 if hit_target else 0, row_id),
        )
        await db.commit()
    except Exception as e:
        log.error("st_signal_outcome_update_error", row_id=row_id, error=str(e))


async def query_st_signals(
    symbol: str | None = None,
    *,
    direction: str | None = None,
    entered: bool | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Query signal_log with optional filters."""
    db = await get_db()
    conditions: list[str] = []
    params: list[Any] = []

    if symbol:
        conditions.append("symbol = ?")
        params.append(symbol.upper())
    if direction:
        conditions.append("direction = ?")
        params.append(direction.upper())
    if entered is not None:
        conditions.append("entered = ?")
        params.append(1 if entered else 0)

    where = " AND ".join(conditions) if conditions else "1=1"
    params.append(limit)

    cursor = await db.execute(
        f"SELECT * FROM signal_log WHERE {where} ORDER BY id DESC LIMIT ?",  # noqa: S608
        params,
    )
    rows = await cursor.fetchall()
    return [dict(row) for row in rows]


async def get_pending_outcomes(
    lookback_minutes: int = 60,
) -> list[dict[str, Any]]:
    """Get entered signals that don't have outcome yet (for stats updater)."""
    db = await get_db()
    cursor = await db.execute(
        """SELECT * FROM signal_log
           WHERE entered = 1
             AND outcome_pct IS NULL
             AND datetime <= datetime('now', ?)
           ORDER BY id""",
        (f"-{lookback_minutes} minutes",),
    )
    rows = await cursor.fetchall()
    return [dict(row) for row in rows]


# ── signal_stats operations ───────────────────────────────

async def get_signal_stats(
    direction: str,
    band: str,
    hour_bucket: int,
) -> dict[str, Any] | None:
    """Get stats for a specific direction + band + hour_bucket combo."""
    db = await get_db()
    cursor = await db.execute(
        """SELECT success_rate, sample_count
           FROM signal_stats
           WHERE direction = ? AND band = ? AND hour_bucket = ?""",
        (direction.upper(), band.upper(), hour_bucket),
    )
    row = await cursor.fetchone()
    if row is None:
        return None
    return dict(row)


async def upsert_signal_stats(
    direction: str,
    band: str,
    hour_bucket: int,
    success_rate: float,
    sample_count: int,
) -> None:
    """Insert or update signal_stats row."""
    db = await get_db()
    await db.execute(
        """INSERT INTO signal_stats (direction, band, hour_bucket, success_rate, sample_count, updated_at)
           VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(direction, band, hour_bucket)
           DO UPDATE SET success_rate = excluded.success_rate,
                         sample_count = excluded.sample_count,
                         updated_at = CURRENT_TIMESTAMP""",
        (direction.upper(), band.upper(), hour_bucket, success_rate, sample_count),
    )
    await db.commit()


async def rebuild_all_stats() -> int:
    """Rebuild signal_stats from signal_log (called by stats updater).

    Groups entered signals with known outcomes by direction + band + hour_bucket.
    Returns number of stat rows upserted.
    """
    db = await get_db()

    cursor = await db.execute(
        """SELECT
             direction,
             band,
             CAST(strftime('%H', datetime) AS INTEGER) AS hour_bucket,
             COUNT(*) AS sample_count,
             SUM(CASE WHEN hit_target = 1 THEN 1 ELSE 0 END) AS hits
           FROM signal_log
           WHERE entered = 1
             AND hit_target IS NOT NULL
           GROUP BY direction, band, hour_bucket""",
    )
    rows = await cursor.fetchall()

    count = 0
    for row in rows:
        rate = row["hits"] / row["sample_count"] if row["sample_count"] > 0 else 0.0
        await upsert_signal_stats(
            direction=row["direction"],
            band=row["band"],
            hour_bucket=row["hour_bucket"],
            success_rate=round(rate, 4),
            sample_count=row["sample_count"],
        )
        count += 1

    return count
