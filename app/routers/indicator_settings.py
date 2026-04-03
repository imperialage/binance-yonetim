"""Indicator settings API — per-symbol indicator parameter management."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.modules.indicator_settings_store import (
    delete_settings,
    get_all_settings,
    get_settings_or_defaults,
    upsert_settings,
)
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()


class IndicatorSettingsBody(BaseModel):
    interval: str | None = None
    rsi_len: int | None = None
    long_thresh: float | None = None
    short_thresh: float | None = None
    max_gap: int | None = None
    entry_buffer: float | None = None
    tp_pct: float | None = None
    sl_pct: float | None = None
    commission: float | None = None
    weekend_closed: int | None = None
    active: int | None = None
    listening: int | None = None


@router.get("/api/indicator-settings")
async def api_get_all() -> dict:
    """Tum sembol indikatör ayarlarini dondur."""
    settings = await get_all_settings()
    return {"settings": settings, "count": len(settings)}


@router.get("/api/indicator-settings/{symbol}")
async def api_get_one(symbol: str) -> dict:
    """Tek sembol ayarini dondur (DB'de yoksa default)."""
    settings = await get_settings_or_defaults(symbol)
    return settings


@router.put("/api/indicator-settings/{symbol}")
async def api_upsert(symbol: str, body: IndicatorSettingsBody) -> dict:
    """Sembol ayarini kaydet veya guncelle."""
    data = {k: v for k, v in body.model_dump().items() if v is not None}
    result = await upsert_settings(symbol, data)
    return result


@router.delete("/api/indicator-settings/{symbol}")
async def api_delete(symbol: str) -> dict:
    """Sembol ayarini sil."""
    deleted = await delete_settings(symbol)
    return {"deleted": deleted, "symbol": symbol.upper()}
