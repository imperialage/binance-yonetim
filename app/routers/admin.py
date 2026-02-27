"""POST /config – admin runtime configuration endpoint."""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException

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
