"""POST /tv-webhook – Redirects to /st-webhook for backward compatibility.

Mevcut TradingView alert'leri /tv-webhook'a gonderiyordu.
Artik tum sinyaller /st-webhook uzerinden islenir.
Bu dosya eski URL'yi yeni endpoint'e yonlendirir.
"""

from __future__ import annotations

from fastapi import APIRouter, Request

from app.routers.st_webhook import st_webhook
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()


@router.post("/tv-webhook")
async def tv_webhook(request: Request):
    """Backward compat: forward /tv-webhook → /st-webhook."""
    log.info("tv_webhook_redirect", path="/tv-webhook -> /st-webhook")
    return await st_webhook(request)
