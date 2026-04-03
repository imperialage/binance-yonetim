"""Indicator settings store — per-symbol indicator parameters (aiosqlite).

Stores RSI length, thresholds, gap, buffer, TP/SL, etc. per symbol.
Data persisted in Railway volume (/data/indicator_settings.db).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import aiosqlite

from app.utils.logging import get_logger

log = get_logger(__name__)

_db: aiosqlite.Connection | None = None
_DATA_DIR = os.getenv("DATA_DIR", "data")
DB_PATH = Path(f"{_DATA_DIR}/indicator_settings.db")

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS indicator_settings (
    symbol             TEXT PRIMARY KEY,
    interval           TEXT NOT NULL DEFAULT '15m',
    rsi_len            INTEGER NOT NULL DEFAULT 10,
    long_thresh        REAL NOT NULL DEFAULT 32,
    short_thresh       REAL NOT NULL DEFAULT 70,
    max_gap            INTEGER NOT NULL DEFAULT 12,
    entry_buffer       REAL NOT NULL DEFAULT 0.1,
    tp_pct             REAL NOT NULL DEFAULT 1.0,
    sl_pct             REAL NOT NULL DEFAULT 0.3,
    commission         REAL NOT NULL DEFAULT 0.08,
    weekend_closed     INTEGER NOT NULL DEFAULT 0,
    active             INTEGER NOT NULL DEFAULT 1,
    listening          INTEGER NOT NULL DEFAULT 1,
    weight             REAL NOT NULL DEFAULT 0.10,
    reverse_signal     INTEGER NOT NULL DEFAULT 0,
    sl_enabled         INTEGER NOT NULL DEFAULT 1,
    allowed_directions TEXT NOT NULL DEFAULT 'BOTH',
    updated_at         TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

# Yeni sutunlar icin migration — mevcut tablo varsa sutun ekle
_MIGRATIONS = [
    "ALTER TABLE indicator_settings ADD COLUMN weight REAL NOT NULL DEFAULT 0.10",
    "ALTER TABLE indicator_settings ADD COLUMN reverse_signal INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE indicator_settings ADD COLUMN sl_enabled INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE indicator_settings ADD COLUMN allowed_directions TEXT NOT NULL DEFAULT 'BOTH'",
]

# Default ayarlar — yeni sembol eklendiginde kullanilir
DEFAULTS = {
    "interval": "15m",
    "rsi_len": 10,
    "long_thresh": 32.0,
    "short_thresh": 70.0,
    "max_gap": 12,
    "entry_buffer": 0.1,
    "tp_pct": 1.0,
    "sl_pct": 0.3,
    "commission": 0.08,
    "weekend_closed": 0,
    "active": 1,
    "listening": 1,
    "weight": 0.10,
    "reverse_signal": 0,
    "sl_enabled": 1,
    "allowed_directions": "BOTH",
}


async def get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _db = await aiosqlite.connect(str(DB_PATH))
        _db.row_factory = aiosqlite.Row
        await _db.execute("PRAGMA journal_mode=WAL;")
        await _db.execute("PRAGMA synchronous=NORMAL;")
    return _db


async def init_indicator_settings_db() -> None:
    db = await get_db()
    await db.execute(_CREATE_TABLE)
    # Mevcut tabloya yeni sutunlar ekle (zaten varsa sessizce atla)
    for migration in _MIGRATIONS:
        try:
            await db.execute(migration)
        except Exception:
            pass  # sutun zaten var
    await db.commit()
    await log.ainfo("indicator_settings_db_ready", path=str(DB_PATH))


async def close_indicator_settings_db() -> None:
    global _db
    if _db is not None:
        await _db.close()
        _db = None


async def get_all_settings() -> list[dict[str, Any]]:
    """Tum sembol ayarlarini dondur."""
    db = await get_db()
    cursor = await db.execute("SELECT * FROM indicator_settings ORDER BY symbol")
    rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def get_settings(symbol: str) -> dict[str, Any] | None:
    """Tek sembol ayarini dondur. Yoksa None."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM indicator_settings WHERE symbol = ?",
        (symbol.upper(),),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_settings_or_defaults(symbol: str) -> dict[str, Any]:
    """Sembol ayarini dondur. DB'de yoksa default degerler."""
    settings = await get_settings(symbol)
    if settings:
        return settings
    return {"symbol": symbol.upper(), **DEFAULTS}


async def upsert_settings(symbol: str, data: dict[str, Any]) -> dict[str, Any]:
    """Ayar kaydet veya guncelle. Sadece gonderilen alanlar guncellenir."""
    db = await get_db()
    sym = symbol.upper()

    # Mevcut ayarlari oku veya default olustur
    existing = await get_settings(sym)

    if existing:
        # Guncelle — sadece gonderilen alanlar
        updates = {}
        for key in DEFAULTS:
            if key in data:
                updates[key] = data[key]

        if updates:
            set_clause = ", ".join(f"{k} = ?" for k in updates)
            values = list(updates.values())
            values.append(sym)
            await db.execute(
                f"UPDATE indicator_settings SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE symbol = ?",  # noqa: S608
                values,
            )
            await db.commit()
    else:
        # Yeni kayit olustur
        row = {**DEFAULTS, **{k: v for k, v in data.items() if k in DEFAULTS}}
        await db.execute(
            """INSERT INTO indicator_settings
               (symbol, interval, rsi_len, long_thresh, short_thresh, max_gap,
                entry_buffer, tp_pct, sl_pct, commission, weekend_closed, active, listening,
                weight, reverse_signal, sl_enabled, allowed_directions)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                sym,
                row["interval"],
                row["rsi_len"],
                row["long_thresh"],
                row["short_thresh"],
                row["max_gap"],
                row["entry_buffer"],
                row["tp_pct"],
                row["sl_pct"],
                row["commission"],
                row["weekend_closed"],
                row["active"],
                row["listening"],
                row["weight"],
                row["reverse_signal"],
                row["sl_enabled"],
                row["allowed_directions"],
            ),
        )
        await db.commit()

    result = await get_settings(sym)
    await log.ainfo("indicator_settings_saved", symbol=sym)
    return result


async def delete_settings(symbol: str) -> bool:
    """Sembol ayarini sil."""
    db = await get_db()
    cursor = await db.execute(
        "DELETE FROM indicator_settings WHERE symbol = ?",
        (symbol.upper(),),
    )
    await db.commit()
    return cursor.rowcount > 0
