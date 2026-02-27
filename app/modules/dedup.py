"""Deduplication and rate-limiting via Redis."""

from __future__ import annotations

import time

import redis.asyncio as aioredis

from app.config import settings
from app.utils.logging import get_logger

log = get_logger(__name__)

DEDUPE_TTL = 600  # 10 minutes


async def is_duplicate(r: aioredis.Redis, event_id: str) -> bool:
    key = f"tv:dedupe:{event_id}"
    existing = await r.get(key)
    if existing:
        return True
    await r.set(key, "1", ex=DEDUPE_TTL)
    return False


async def check_rate_limit(r: aioredis.Redis, symbol: str) -> bool:
    """Return True if rate limit exceeded."""
    window = settings.rate_limit_window_sec
    max_events = settings.rate_limit_max_events
    bucket = int(time.time()) // window
    key = f"tv:rate:{symbol}:{bucket}"

    count = await r.incr(key)
    if count == 1:
        await r.expire(key, window * 2)

    if count > max_events:
        log.warning("rate_limit_exceeded", symbol=symbol, count=count, window=window)
        return True
    return False
