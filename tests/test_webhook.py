"""Tests for webhook endpoint, dedup, and rules engine."""

from __future__ import annotations

import pytest

from tests.conftest import make_payload


# ── Original webhook tests ───────────────────────────

@pytest.mark.asyncio
async def test_webhook_accept(client):
    """Valid webhook payload should be accepted with rules result."""
    payload = make_payload(event_id="evt_001")
    resp = await client.post("/tv-webhook", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "accepted"
    assert data["event_id"] == "evt_001"
    assert data["decision"] is not None
    assert data["bias"] is not None
    assert data["confidence"] is not None


@pytest.mark.asyncio
async def test_webhook_invalid_secret(client):
    """Wrong secret should return 401."""
    payload = make_payload(secret="wrong_secret", event_id="evt_002")
    resp = await client.post("/tv-webhook", json=payload)
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_webhook_duplicate(client):
    """Same event_id sent twice should be flagged as duplicate."""
    payload = make_payload(event_id="evt_dup_001")
    resp1 = await client.post("/tv-webhook", json=payload)
    assert resp1.json()["status"] == "accepted"

    resp2 = await client.post("/tv-webhook", json=payload)
    assert resp2.json()["status"] == "duplicate"


@pytest.mark.asyncio
async def test_webhook_missing_fields(client):
    """Payload missing required fields should return 422."""
    resp = await client.post("/tv-webhook", json={"secret": "test_secret"})
    assert resp.status_code == 422


# ── Rules engine unit tests ──────────────────────────

def test_rules_engine_long_bias():
    """Strong BUY signals should produce LONG_SETUP."""
    from app.modules.rules_engine import evaluate
    from app.schemas.config import RuntimeConfig
    from app.schemas.evaluation import AggregationResult, IndicatorSignal, TimeframeSummary

    config = RuntimeConfig()
    agg = AggregationResult(
        symbol="ETHUSDT",
        timeframes={
            "4h": TimeframeSummary(
                tf="4h", buy_count=2, sell_count=0,
                indicators=[
                    IndicatorSignal(indicator="BigBeluga", signal="BUY", strength=0.9, ts=1000),
                    IndicatorSignal(indicator="ChartPrime", signal="BUY", strength=0.8, ts=1000),
                ],
            ),
            "1h": TimeframeSummary(
                tf="1h", buy_count=1, sell_count=0,
                indicators=[
                    IndicatorSignal(indicator="BigBeluga", signal="BUY", strength=0.7, ts=1000),
                ],
            ),
            "15m": TimeframeSummary(
                tf="15m", buy_count=1, sell_count=0,
                indicators=[
                    IndicatorSignal(indicator="SwiftAlgo", signal="BUY", strength=0.6, ts=1000),
                ],
            ),
        },
        used_events=[],
        aggregated_at=1000,
    )

    result = evaluate(agg, config)
    assert result.bias == "LONG"
    assert result.decision == "LONG_SETUP"
    assert result.confidence > 0
    assert result.score > config.threshold


def test_rules_engine_veto():
    """4H SELL should veto LONG_SETUP."""
    from app.modules.rules_engine import evaluate
    from app.schemas.config import RuntimeConfig
    from app.schemas.evaluation import AggregationResult, IndicatorSignal, TimeframeSummary

    config = RuntimeConfig()
    agg = AggregationResult(
        symbol="ETHUSDT",
        timeframes={
            "4h": TimeframeSummary(
                tf="4h", buy_count=0, sell_count=2,
                indicators=[
                    IndicatorSignal(indicator="BigBeluga", signal="SELL", strength=0.9, ts=1000),
                ],
            ),
            "1h": TimeframeSummary(
                tf="1h", buy_count=2, sell_count=0,
                indicators=[
                    IndicatorSignal(indicator="BigBeluga", signal="BUY", strength=0.9, ts=1000),
                    IndicatorSignal(indicator="ChartPrime", signal="BUY", strength=0.9, ts=1000),
                ],
            ),
            "15m": TimeframeSummary(
                tf="15m", buy_count=2, sell_count=0,
                indicators=[
                    IndicatorSignal(indicator="SwiftAlgo", signal="BUY", strength=0.9, ts=1000),
                    IndicatorSignal(indicator="BigBeluga", signal="BUY", strength=0.9, ts=1000),
                ],
            ),
        },
        used_events=[],
        aggregated_at=1000,
    )

    result = evaluate(agg, config)
    if result.bias == "LONG":
        assert result.veto_applied is True
        assert result.decision == "NO_TRADE"
    else:
        assert result.decision in ("WATCH", "SHORT_SETUP")


def test_rules_engine_neutral():
    """Mixed signals should produce WATCH."""
    from app.modules.rules_engine import evaluate
    from app.schemas.config import RuntimeConfig
    from app.schemas.evaluation import AggregationResult, IndicatorSignal, TimeframeSummary

    config = RuntimeConfig()
    agg = AggregationResult(
        symbol="ETHUSDT",
        timeframes={
            "4h": TimeframeSummary(
                tf="4h", buy_count=1, sell_count=1,
                indicators=[
                    IndicatorSignal(indicator="BigBeluga", signal="BUY", strength=0.5, ts=1000),
                    IndicatorSignal(indicator="ChartPrime", signal="SELL", strength=0.5, ts=1000),
                ],
            ),
            "1h": TimeframeSummary(tf="1h"),
            "15m": TimeframeSummary(tf="15m"),
        },
        used_events=[],
        aggregated_at=1000,
    )

    result = evaluate(agg, config)
    assert result.bias == "NEUTRAL"
    assert result.decision == "WATCH"
