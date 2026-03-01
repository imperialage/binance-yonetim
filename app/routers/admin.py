"""Admin endpoints – runtime config & event management."""

from __future__ import annotations

import json

from fastapi import APIRouter, Header, HTTPException, Query

from app.config import settings
from app.modules.redis_client import get_redis
from app.schemas.config import RuntimeConfig
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()


@router.post("/config", response_model=RuntimeConfig)
async def update_config(
    body: RuntimeConfig,
    x_admin_token: str = Header(...),
) -> RuntimeConfig:
    if x_admin_token != settings.admin_token:
        raise HTTPException(status_code=401, detail="Invalid admin token")

    r = await get_redis()
    await r.set("tv:config", body.model_dump_json())

    log.info("config_updated", config=body.model_dump())
    return body


@router.delete("/events/{symbol}")
async def delete_event(
    symbol: str,
    event_id: str = Query(...),
    x_admin_token: str = Header(...),
) -> dict:
    if x_admin_token != settings.admin_token:
        raise HTTPException(status_code=401, detail="Invalid admin token")

    r = await get_redis()
    key = f"tv:events:{symbol.upper()}"
    raw_list = await r.lrange(key, 0, -1)

    removed = 0
    for raw in raw_list:
        try:
            ev = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        if ev.get("event_id") == event_id:
            await r.lrem(key, 1, raw)
            removed += 1

    log.info("event_deleted", symbol=symbol, event_id=event_id, removed=removed)
    return {"deleted": removed, "event_id": event_id}
