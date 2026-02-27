"""Async Redis client lifecycle management."""

from __future__ import annotations

import redis.asyncio as aioredis

from app.config import settings
from app.utils.logging import get_logger

log = get_logger(__name__)

_pool: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _pool
    if _pool is None:
        _pool = aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=20,
        )
        await log.ainfo("redis_connected", url=settings.redis_url.split("@")[-1])
    return _pool


async def close_redis() -> None:
    global _pool
    if _pool is not None:
        await _pool.aclose()  # type: ignore[union-attr]
        _pool = None
        await log.ainfo("redis_closed")


async def redis_ping() -> bool:
    try:
        r = await get_redis()
        return await r.ping()  # type: ignore[return-value]
    except Exception:
        return False
