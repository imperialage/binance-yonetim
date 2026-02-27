"""Redis-based distributed lock helpers (single-flight pattern)."""

from __future__ import annotations

import uuid

import redis.asyncio as aioredis

from app.utils.logging import get_logger

log = get_logger(__name__)

AI_LOCK_TTL_MS = 60_000  # 60 seconds


async def acquire_ai_lock(r: aioredis.Redis, symbol: str) -> str | None:
    """Try to acquire the AI generation lock for *symbol*.

    Returns a unique lock_value on success (needed for release), or None if
    the lock is already held.
    """
    key = f"tv:lock:ai:{symbol}"
    lock_value = uuid.uuid4().hex
    acquired = await r.set(key, lock_value, nx=True, px=AI_LOCK_TTL_MS)
    if acquired:
        return lock_value
    return None


async def release_ai_lock(r: aioredis.Redis, symbol: str, lock_value: str) -> None:
    """Release the lock only if we still own it (compare-and-delete via Lua)."""
    key = f"tv:lock:ai:{symbol}"
    lua = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    """
    await r.eval(lua, 1, key, lock_value)  # type: ignore[arg-type]
