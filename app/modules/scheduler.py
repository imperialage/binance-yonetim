"""Scheduled refresh: keep evaluations fresh even when no webhooks arrive.

Runs as an asyncio background task inside the FastAPI lifespan.
"""

from __future__ import annotations

import asyncio
import time

from app.modules.aggregator import aggregate, load_runtime_config
from app.modules.ai_client import create_ai_client
from app.modules.locks import acquire_ai_lock, release_ai_lock
from app.modules.market_data import get_market_summaries
from app.modules.redis_client import get_redis
from app.modules.rules_engine import evaluate
from app.schemas.evaluation import (
    Evaluation,
    LatestAI,
    LatestEvaluation,
    LatestRules,
)
from app.utils.logging import get_logger

log = get_logger(__name__)

LATEST_TTL = 172_800  # 48 h

_task: asyncio.Task[None] | None = None


# ── helpers shared by webhook + scheduler ────────────

def build_latest_rules(rules_out, agg) -> LatestRules:
    signals_used = []
    aggregated_counts: dict[str, dict[str, int]] = {}
    for tf, ts in agg.timeframes.items():
        signals_used.extend(ts.indicators)
        aggregated_counts[tf] = {
            "buy": ts.buy_count,
            "sell": ts.sell_count,
            "close": ts.close_count,
            "neutral": ts.neutral_count,
        }
    return LatestRules(
        decision=rules_out.decision,
        bias=rules_out.bias,
        confidence=rules_out.confidence,
        score=rules_out.score,
        reasons=rules_out.reasons,
        signals_used=signals_used,
        aggregated_counts=aggregated_counts,
    )


async def store_latest(
    symbol: str,
    rules_out,
    agg,
    market,
    ai_text: str | None,
    evaluation_id: str | None = None,
) -> None:
    """Persist the two-layer LatestEvaluation into Redis."""
    r = await get_redis()
    now = int(time.time())

    lr = build_latest_rules(rules_out, agg)

    latest_ai: LatestAI | None = None
    if ai_text:
        lines = [l.strip() for l in ai_text.strip().splitlines() if l.strip()][:6]
        latest_ai = LatestAI(lines=lines, generated_at=now)

    # If no new AI, try to preserve the previous one
    if latest_ai is None:
        prev_raw = await r.get(f"tv:latest:{symbol}")
        if prev_raw:
            try:
                prev = LatestEvaluation.model_validate_json(prev_raw)
                latest_ai = prev.latest_ai
            except Exception:
                pass

    import uuid as _uuid
    eid = evaluation_id or _uuid.uuid4().hex[:12]

    le = LatestEvaluation(
        evaluation_id=eid,
        symbol=symbol,
        latest_rules=lr,
        latest_ai=latest_ai,
        market_summary=market if market else None,
        evaluated_at=now,
    )

    # Compare-and-set: only overwrite if our ts is newer
    prev_raw = await r.get(f"tv:latest:{symbol}")
    if prev_raw:
        try:
            prev = LatestEvaluation.model_validate_json(prev_raw)
            if prev.evaluated_at > now:
                return  # stale – skip
        except Exception:
            pass

    await r.set(f"tv:latest:{symbol}", le.model_dump_json(), ex=LATEST_TTL)


# ── Scheduler loop ──────────────────────────────────

async def _tick(symbol: str, force_ai: bool) -> None:
    """Single refresh tick for one symbol."""
    try:
        r = await get_redis()
        config = await load_runtime_config(r)

        agg = await aggregate(r, symbol, config, max_events=config.events_max_per_symbol)
        rules_out = evaluate(agg, config)

        market = await get_market_summaries(symbol)

        ai_text: str | None = None
        if force_ai:
            lock_val = await acquire_ai_lock(r, symbol)
            if lock_val:
                try:
                    ai = create_ai_client()
                    ai_text = await ai.explain(rules_out, agg, market)
                finally:
                    await release_ai_lock(r, symbol, lock_val)
            else:
                log.debug("scheduler_ai_lock_busy", symbol=symbol)

        await store_latest(symbol, rules_out, agg, market, ai_text)
        log.debug("scheduler_tick", symbol=symbol, decision=rules_out.decision, ai=force_ai)

    except Exception as e:
        log.error("scheduler_tick_error", symbol=symbol, error=str(e))


async def _loop() -> None:
    """Main scheduler loop – runs until cancelled."""
    log.info("scheduler_started")
    rules_counters: dict[str, int] = {}  # symbol -> ticks since last AI

    try:
        while True:
            r = await get_redis()
            config = await load_runtime_config(r)

            interval = max(config.refresh_rules_seconds, 5)
            ai_every = max(config.refresh_ai_seconds // interval, 1)

            for symbol in config.watchlist_symbols:
                count = rules_counters.get(symbol, 0) + 1
                force_ai = count >= ai_every
                if force_ai:
                    count = 0
                rules_counters[symbol] = count

                await _tick(symbol, force_ai)

            await asyncio.sleep(interval)

    except asyncio.CancelledError:
        log.info("scheduler_stopped")


def start_scheduler() -> asyncio.Task[None]:
    global _task
    _task = asyncio.create_task(_loop())
    return _task


async def stop_scheduler() -> None:
    global _task
    if _task is not None and not _task.done():
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        _task = None
