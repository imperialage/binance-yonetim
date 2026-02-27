"""Pytest fixtures for Market Intelligence Service tests."""

from __future__ import annotations

import fnmatch
import os
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

# Override settings before importing app
os.environ["TV_WEBHOOK_SECRET"] = "test_secret"
os.environ["ADMIN_TOKEN"] = "test_admin"
os.environ["REDIS_URL"] = "redis://localhost:6379/1"
os.environ["AI_PROVIDER"] = "dummy"
os.environ["LOG_JSON"] = "false"


class FakeRedis:
    """In-memory fake Redis for tests – no real Redis required."""

    def __init__(self) -> None:
        self._data: dict[str, str | list[str]] = {}
        self._ttls: dict[str, int] = {}

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        val = self._data.get(key)
        if isinstance(val, str):
            return val
        return None

    async def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
        px: int | None = None,
    ) -> bool | None:
        if nx and key in self._data:
            return None
        self._data[key] = value
        if ex:
            self._ttls[key] = ex
        if px:
            self._ttls[key] = px // 1000
        return True

    async def incr(self, key: str) -> int:
        current = self._data.get(key, "0")
        new_val = (int(current) if isinstance(current, str) else 0) + 1
        self._data[key] = str(new_val)
        return new_val

    async def expire(self, key: str, seconds: int) -> None:
        self._ttls[key] = seconds

    async def rpush(self, key: str, *values: str) -> int:
        if key not in self._data or not isinstance(self._data[key], list):
            self._data[key] = []
        lst = self._data[key]
        assert isinstance(lst, list)
        lst.extend(values)
        return len(lst)

    async def ltrim(self, key: str, start: int, stop: int) -> None:
        val = self._data.get(key)
        if not isinstance(val, list):
            return
        length = len(val)
        s = max(length + start, 0) if start < 0 else start
        e = (length + stop) if stop < 0 else stop
        self._data[key] = val[s : e + 1]

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        val = self._data.get(key, [])
        if not isinstance(val, list):
            return []
        length = len(val)
        s = max(length + start, 0) if start < 0 else start
        e = (length + end) if end < 0 else end
        return val[s : e + 1]

    async def llen(self, key: str) -> int:
        val = self._data.get(key, [])
        return len(val) if isinstance(val, list) else 0

    async def delete(self, *keys: str) -> int:
        count = 0
        for k in keys:
            if k in self._data:
                del self._data[k]
                count += 1
        return count

    async def eval(self, script: str, numkeys: int, *args: str) -> int:
        """Minimal Lua emulation for compare-and-delete lock pattern."""
        if numkeys == 1 and len(args) >= 2:
            key, expected = args[0], args[1]
            current = self._data.get(key)
            if isinstance(current, str) and current == expected:
                del self._data[key]
                return 1
        return 0

    async def scan_iter(self, match: str = "*", count: int = 100):
        for key in list(self._data.keys()):
            if fnmatch.fnmatch(key, match):
                yield key

    async def aclose(self) -> None:
        pass


_fake_redis = FakeRedis()


@pytest.fixture(autouse=True)
def reset_fake_redis():
    _fake_redis._data.clear()
    _fake_redis._ttls.clear()


@pytest.fixture
def fake_redis():
    """Expose FakeRedis for direct unit tests."""
    return _fake_redis


@pytest.fixture
async def client():
    """Async test client with all Redis / external calls patched."""
    from app.schemas.config import RuntimeConfig

    cfg = RuntimeConfig()
    redis_mock = AsyncMock(return_value=_fake_redis)

    with (
        # Redis
        patch("app.modules.redis_client.get_redis", redis_mock),
        patch("app.routers.webhook.get_redis", redis_mock),
        patch("app.routers.events.get_redis", redis_mock),
        patch("app.routers.latest.get_redis", redis_mock),
        patch("app.routers.status.get_redis", redis_mock),
        patch("app.routers.status.redis_ping", AsyncMock(return_value=True)),
        patch("app.routers.admin.get_redis", redis_mock),
        # Config
        patch("app.modules.aggregator.load_runtime_config", AsyncMock(return_value=cfg)),
        patch("app.routers.webhook.load_runtime_config", AsyncMock(return_value=cfg)),
        # Market data
        patch("app.routers.webhook.get_last_price", AsyncMock(return_value=3500.0)),
        # Scheduler (don't start real loop)
        patch("app.modules.scheduler.start_scheduler", return_value=None),
        patch("app.modules.scheduler.stop_scheduler", AsyncMock(return_value=None)),
        # Background AI store – let it write through to FakeRedis
        patch("app.routers.webhook._background_evaluation", AsyncMock(return_value=None)),
    ):
        from app.main import app

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


def make_payload(
    indicator: str = "BigBeluga",
    symbol: str = "ETHUSDT",
    tf: str = "15m",
    signal: str = "BUY",
    strength: float = 0.8,
    event_id: str | None = None,
    secret: str = "test_secret",
) -> dict:
    payload: dict = {
        "secret": secret,
        "indicator": indicator,
        "symbol": symbol,
        "tf": tf,
        "signal": signal,
        "strength": strength,
    }
    if event_id:
        payload["event_id"] = event_id
    return payload
