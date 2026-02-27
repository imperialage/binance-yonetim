"""Tests for enhancements: events endpoint, LTRIM, scheduler, locks, tf normalization,
symbol prefix, signal validation, ts/price parsing."""

from __future__ import annotations

import json
import time

import pytest

from tests.conftest import make_payload


# ── 1) /events endpoint basic ───────────────────────

@pytest.mark.asyncio
async def test_events_endpoint_basic(client, fake_redis):
    """GET /events should return stored events."""
    for i in range(3):
        payload = make_payload(event_id=f"ev_basic_{i}", indicator="BigBeluga", tf="15m", signal="BUY")
        await client.post("/tv-webhook", json=payload)

    resp = await client.get("/events", params={"symbol": "ETHUSDT", "limit": 10})
    assert resp.status_code == 200
    data = resp.json()
    assert data["symbol"] == "ETHUSDT"
    assert data["count"] == 3
    assert len(data["events"]) == 3
    assert data["events"][0]["event_id"] == "ev_basic_2"


# ── 2) /events filters ──────────────────────────────

@pytest.mark.asyncio
async def test_events_endpoint_filters(client, fake_redis):
    """GET /events should filter by indicator, tf, signal."""
    payloads = [
        make_payload(event_id="f1", indicator="BigBeluga", tf="15m", signal="BUY"),
        make_payload(event_id="f2", indicator="ChartPrime", tf="1h", signal="SELL"),
        make_payload(event_id="f3", indicator="BigBeluga", tf="1h", signal="SELL"),
        make_payload(event_id="f4", indicator="SwiftAlgo", tf="4h", signal="BUY"),
    ]
    for p in payloads:
        await client.post("/tv-webhook", json=p)

    resp = await client.get("/events", params={"symbol": "ETHUSDT", "indicator": "BigBeluga"})
    data = resp.json()
    assert all(e["indicator"] == "BigBeluga" for e in data["events"])
    assert data["count"] == 2

    resp = await client.get("/events", params={"symbol": "ETHUSDT", "tf": "1h"})
    data = resp.json()
    assert all(e["tf"] == "1h" for e in data["events"])
    assert data["count"] == 2

    resp = await client.get("/events", params={"symbol": "ETHUSDT", "signal": "BUY"})
    data = resp.json()
    assert all(e["signal"] == "BUY" for e in data["events"])
    assert data["count"] == 2


# ── 3) LTRIM works (events_max_per_symbol) ───────────

@pytest.mark.asyncio
async def test_ltrim_limits_events(fake_redis):
    """After RPUSH + LTRIM, list should be capped."""
    key = "tv:events:TESTUSDT"
    max_items = 5
    for i in range(10):
        event = json.dumps({"event_id": f"trim_{i}", "ts": 1000 + i, "tf": "15m", "signal": "BUY"})
        await fake_redis.rpush(key, event)
    await fake_redis.ltrim(key, -max_items, -1)

    result = await fake_redis.lrange(key, 0, -1)
    assert len(result) == max_items
    assert json.loads(result[0])["event_id"] == "trim_5"
    assert json.loads(result[-1])["event_id"] == "trim_9"


# ── 4) Scheduler tick ───────────────────────────────

@pytest.mark.asyncio
async def test_scheduler_tick(fake_redis):
    """Scheduler _tick should produce a latest evaluation."""
    from unittest.mock import AsyncMock, patch

    from app.schemas.evaluation import MarketSummary

    mock_market = {
        "15m": MarketSummary(tf="15m", last_price=3500, green_candles=12, red_candles=8, slope=10.0),
        "1h": MarketSummary(tf="1h", last_price=3500, green_candles=10, red_candles=10, slope=-5.0),
        "4h": MarketSummary(tf="4h", last_price=3500, green_candles=14, red_candles=6, slope=20.0),
    }

    event = json.dumps({
        "event_id": "sched_1", "ts": int(time.time()), "indicator": "BigBeluga",
        "symbol": "ETHUSDT", "tf": "4h", "signal": "BUY", "strength": 0.9,
    })
    await fake_redis.rpush("tv:events:ETHUSDT", event)

    async def _fake_get_redis():
        return fake_redis

    with (
        patch("app.modules.scheduler.get_redis", side_effect=_fake_get_redis),
        patch("app.modules.aggregator.load_runtime_config") as mc,
        patch("app.modules.scheduler.load_runtime_config") as mc2,
        patch("app.modules.scheduler.get_market_summaries", return_value=mock_market),
        patch("app.modules.scheduler.acquire_ai_lock", return_value="fake_lock"),
        patch("app.modules.scheduler.release_ai_lock", return_value=None),
    ):
        from app.schemas.config import RuntimeConfig
        cfg = RuntimeConfig()
        mc.return_value = cfg
        mc2.return_value = cfg

        from app.modules.scheduler import _tick
        await _tick("ETHUSDT", force_ai=True)

    raw = await fake_redis.get("tv:latest:ETHUSDT")
    assert raw is not None
    latest = json.loads(raw)
    assert latest["symbol"] == "ETHUSDT"
    assert latest["latest_rules"]["decision"] in ("LONG_SETUP", "SHORT_SETUP", "WATCH", "NO_TRADE")


# ── 5) AI single-flight lock ────────────────────────

@pytest.mark.asyncio
async def test_ai_single_flight_lock(fake_redis):
    """Only one AI lock should be acquired per symbol; second attempt returns None."""
    from app.modules.locks import acquire_ai_lock, release_ai_lock

    lock1 = await acquire_ai_lock(fake_redis, "ETHUSDT")
    assert lock1 is not None

    lock2 = await acquire_ai_lock(fake_redis, "ETHUSDT")
    assert lock2 is None

    await release_ai_lock(fake_redis, "ETHUSDT", lock1)
    lock3 = await acquire_ai_lock(fake_redis, "ETHUSDT")
    assert lock3 is not None


# ── 6) Timeframe normalization ───────────────────────

def test_tf_normalization():
    from app.modules.normalizer import normalize_tf

    assert normalize_tf("15") == "15m"
    assert normalize_tf("15m") == "15m"
    assert normalize_tf("60") == "1h"
    assert normalize_tf("1h") == "1h"
    assert normalize_tf("1H") == "1h"
    assert normalize_tf("240") == "4h"
    assert normalize_tf("4h") == "4h"
    assert normalize_tf("4H") == "4h"
    assert normalize_tf("  15m  ") == "15m"

    assert normalize_tf("3h") is None
    assert normalize_tf("daily") is None
    assert normalize_tf("") is None


@pytest.mark.asyncio
async def test_webhook_invalid_tf(client):
    """Webhook with invalid timeframe should return 400."""
    payload = make_payload(event_id="bad_tf_001", tf="3h")
    resp = await client.post("/tv-webhook", json=payload)
    assert resp.status_code == 400
    assert "Invalid timeframe" in resp.json()["detail"]


# ── 7) Symbol prefix normalization ───────────────────

def test_symbol_prefix_normalization():
    """Exchange prefixes and .P suffix should be stripped."""
    from app.modules.normalizer import normalize_symbol

    assert normalize_symbol("BINANCE:ETHUSDT") == "ETHUSDT"
    assert normalize_symbol("BINANCE:ETHUSDT.P") == "ETHUSDT"
    assert normalize_symbol("BYBIT:BTCUSDT.P") == "BTCUSDT"
    assert normalize_symbol("  ethusdt  ") == "ETHUSDT"
    assert normalize_symbol("ETHUSDT") == "ETHUSDT"
    assert normalize_symbol("OKX:SOLUSDT") == "SOLUSDT"


@pytest.mark.asyncio
async def test_webhook_symbol_prefix(client):
    """Webhook with 'BINANCE:ETHUSDT' should normalize to 'ETHUSDT'."""
    payload = make_payload(event_id="prefix_001", symbol="BINANCE:ETHUSDT")
    resp = await client.post("/tv-webhook", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "accepted"
    assert data["event_id"] == "prefix_001"


# ── 8) Signal validation (strict: only BUY/SELL) ────

@pytest.mark.asyncio
async def test_webhook_invalid_signal(client):
    """Signal other than BUY/SELL should return 400."""
    payload = make_payload(event_id="badsig_001", signal="HOLD")
    resp = await client.post("/tv-webhook", json=payload)
    assert resp.status_code == 400
    assert "Invalid signal" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_webhook_neutral_signal_rejected(client):
    """NEUTRAL is not a valid TV indicator signal, should 400."""
    payload = make_payload(event_id="neut_001", signal="NEUTRAL")
    resp = await client.post("/tv-webhook", json=payload)
    assert resp.status_code == 400


# ── 9) ts / price string parsing ────────────────────

@pytest.mark.asyncio
async def test_webhook_string_ts_price(client):
    """ts and price sent as strings (TradingView default) should be parsed."""
    payload = {
        "secret": "test_secret",
        "indicator": "BigBeluga",
        "symbol": "ETHUSDT",
        "tf": "15m",
        "signal": "BUY",
        "ts": "1700000000",
        "price": "3521.50",
    }
    resp = await client.post("/tv-webhook", json=payload)
    assert resp.status_code == 200
    assert resp.json()["status"] == "accepted"


@pytest.mark.asyncio
async def test_webhook_unparseable_ts(client):
    """Non-numeric ts should return 400."""
    payload = {
        "secret": "test_secret",
        "indicator": "BigBeluga",
        "symbol": "ETHUSDT",
        "tf": "15m",
        "signal": "BUY",
        "ts": "not_a_number",
        "price": "3500",
    }
    resp = await client.post("/tv-webhook", json=payload)
    assert resp.status_code == 400
    assert "Cannot parse ts" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_webhook_unparseable_price(client):
    """Non-numeric price should return 400."""
    payload = {
        "secret": "test_secret",
        "indicator": "BigBeluga",
        "symbol": "ETHUSDT",
        "tf": "15m",
        "signal": "BUY",
        "price": "abc",
    }
    resp = await client.post("/tv-webhook", json=payload)
    assert resp.status_code == 400
    assert "Cannot parse price" in resp.json()["detail"]


# ── 10) Secret validation returns 401 ───────────────

@pytest.mark.asyncio
async def test_webhook_wrong_secret_401(client):
    """Wrong secret should now return HTTP 401."""
    payload = make_payload(secret="wrong", event_id="sec401")
    resp = await client.post("/tv-webhook", json=payload)
    assert resp.status_code == 401
