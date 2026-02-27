"""Aggregate events within time windows per symbol and timeframe."""

from __future__ import annotations

import json
import time
from typing import Any

import redis.asyncio as aioredis

from app.schemas.config import RuntimeConfig
from app.schemas.evaluation import AggregationResult, IndicatorSignal, TimeframeSummary
from app.utils.logging import get_logger

log = get_logger(__name__)


async def load_runtime_config(r: aioredis.Redis) -> RuntimeConfig:
    raw = await r.get("tv:config")
    if raw:
        return RuntimeConfig.model_validate_json(raw)
    return RuntimeConfig()


async def aggregate(
    r: aioredis.Redis,
    symbol: str,
    config: RuntimeConfig | None = None,
    max_events: int = 1000,
) -> AggregationResult:
    if config is None:
        config = await load_runtime_config(r)

    now = int(time.time())
    windows = config.tf_windows

    # Only fetch the last max_events (optimised LRANGE from tail)
    raw_events = await r.lrange(f"tv:events:{symbol}", -max_events, -1)
    events: list[dict[str, Any]] = []
    for raw in raw_events:
        try:
            events.append(json.loads(raw))
        except (json.JSONDecodeError, TypeError):
            continue

    tf_summaries: dict[str, TimeframeSummary] = {}
    used_events: list[dict[str, Any]] = []

    for tf, window_sec in windows.items():
        cutoff = now - window_sec
        tf_events = [e for e in events if e.get("tf") == tf and e.get("ts", 0) >= cutoff]

        summary = TimeframeSummary(tf=tf)
        latest_per_indicator: dict[str, dict[str, Any]] = {}

        for ev in tf_events:
            sig = ev.get("signal", "NEUTRAL").upper()
            if sig == "BUY":
                summary.buy_count += 1
            elif sig == "SELL":
                summary.sell_count += 1
            elif sig == "CLOSE":
                summary.close_count += 1
            else:
                summary.neutral_count += 1

            ind_name = ev.get("indicator", "unknown")
            prev = latest_per_indicator.get(ind_name)
            if prev is None or ev.get("ts", 0) >= prev.get("ts", 0):
                latest_per_indicator[ind_name] = ev

        for ind_name, ev in latest_per_indicator.items():
            summary.indicators.append(
                IndicatorSignal(
                    indicator=ind_name,
                    signal=ev.get("signal", "NEUTRAL"),
                    strength=ev.get("strength", 0.5),
                    ts=ev.get("ts", 0),
                )
            )

        tf_summaries[tf] = summary
        used_events.extend(tf_events)

    return AggregationResult(
        symbol=symbol,
        timeframes=tf_summaries,
        used_events=used_events,
        aggregated_at=now,
    )
