"""GET /status – health check endpoint."""

from __future__ import annotations

import time

from fastapi import APIRouter
from pydantic import BaseModel

from app.modules.redis_client import get_redis, redis_ping
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()


class StatusResponse(BaseModel):
    status: str
    redis_ok: bool
    events_last_minute: int
    uptime_seconds: int


_start_time = time.time()


@router.get("/status", response_model=StatusResponse)
async def status() -> StatusResponse:
    redis_ok = await redis_ping()

    # Count events in the last minute across known rate buckets
    events_count = 0
    try:
        r = await get_redis()
        bucket = int(time.time()) // 10
        # Scan recent rate keys (last 6 buckets = ~60 seconds)
        for offset in range(6):
            pattern = f"tv:rate:*:{bucket - offset}"
            async for key in r.scan_iter(match=pattern, count=100):
                val = await r.get(key)
                if val:
                    events_count += int(val)
    except Exception:
        pass

    return StatusResponse(
        status="ok" if redis_ok else "degraded",
        redis_ok=redis_ok,
        events_last_minute=events_count,
        uptime_seconds=int(time.time() - _start_time),
    )
