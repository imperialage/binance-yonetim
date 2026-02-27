"""POST /tv-webhook – TradingView webhook receiver."""

from __future__ import annotations

import asyncio
import json
import time

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.config import settings
from app.modules.aggregator import aggregate, load_runtime_config
from app.modules.ai_client import create_ai_client
from app.modules.dedup import check_rate_limit, is_duplicate
from app.modules.locks import acquire_ai_lock, release_ai_lock
from app.modules.market_data import get_last_price, get_market_summaries
from app.modules.normalizer import NormalizeError, normalize
from app.modules.redis_client import get_redis
from app.modules.rules_engine import evaluate
from app.modules.scheduler import store_latest
from app.schemas.webhook import WebhookPayload, WebhookResponse
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()

EVENT_TTL = 86400  # 24h


async def _background_evaluation(symbol: str, rules_output, aggregation_result) -> None:
    """Fetch market data, run AI explanation with single-flight lock, store evaluation."""
    try:
        r = await get_redis()

        market = await get_market_summaries(symbol)

        ai_text: str | None = None
        lock_val = await acquire_ai_lock(r, symbol)
        if lock_val:
            try:
                ai = create_ai_client()
                ai_text = await ai.explain(rules_output, aggregation_result, market)
            finally:
                await release_ai_lock(r, symbol, lock_val)
        else:
            log.info("ai_lock_busy_skipping", symbol=symbol)

        await store_latest(symbol, rules_output, aggregation_result, market, ai_text)
        log.info("evaluation_stored", symbol=symbol, decision=rules_output.decision)

    except Exception as e:
        log.error("background_evaluation_error", symbol=symbol, error=str(e))


@router.post("/tv-webhook", response_model=WebhookResponse)
async def tv_webhook(request: Request) -> WebhookResponse | JSONResponse:
    # TradingView sends Content-Type: text/plain, not application/json
    raw_body = await request.body()
    try:
        body = json.loads(raw_body)
    except (json.JSONDecodeError, ValueError):
        return JSONResponse(status_code=400, content={"detail": "Invalid JSON body"})
    payload = WebhookPayload.model_validate(body)

    # ── Secret validation ────────────────────────────
    if payload.secret != settings.tv_webhook_secret:
        log.warning("invalid_secret", indicator=payload.indicator, symbol=payload.symbol)
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid secret"},
        )

    r = await get_redis()

    # ── Normalize (symbol prefix, tf, signal, ts/price parsing) ──
    fallback_price = 0.0
    if payload.price is None:
        from app.modules.normalizer import normalize_symbol
        fallback_price = await get_last_price(normalize_symbol(payload.symbol))

    result = normalize(payload, fallback_price=fallback_price)

    if isinstance(result, NormalizeError):
        return JSONResponse(status_code=400, content={"detail": result.detail})

    event = result

    # ── Dedupe ───────────────────────────────────────
    if await is_duplicate(r, event.event_id):
        log.info("duplicate_event", event_id=event.event_id)
        return WebhookResponse(status="duplicate", event_id=event.event_id, message="duplicate event")

    # ── Rate limit ───────────────────────────────────
    if await check_rate_limit(r, event.symbol):
        return WebhookResponse(status="rate_limited", event_id=event.event_id, message="rate limit exceeded")

    # ── Store event + LTRIM ──────────────────────────
    config = await load_runtime_config(r)
    event_json = event.model_dump_json()
    key = f"tv:events:{event.symbol}"
    await r.rpush(key, event_json)
    await r.ltrim(key, -config.events_max_per_symbol, -1)
    await r.expire(key, EVENT_TTL)

    log.info(
        "event_stored",
        event_id=event.event_id,
        indicator=event.indicator,
        symbol=event.symbol,
        tf=event.tf,
        signal=event.signal.value,
    )

    # ── Aggregation + Rules (synchronous) ────────────
    agg = await aggregate(r, event.symbol, config, max_events=config.events_max_per_symbol)
    rules_result = evaluate(agg, config)

    # ── Background: market data + AI + store ─────────
    asyncio.create_task(_background_evaluation(event.symbol, rules_result, agg))

    return WebhookResponse(
        status="accepted",
        event_id=event.event_id,
        decision=rules_result.decision,
        bias=rules_result.bias,
        confidence=rules_result.confidence,
        score=rules_result.score,
    )
