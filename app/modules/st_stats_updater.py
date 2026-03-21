"""Periodic signal_stats updater — follows scheduler.py background task pattern.

Every 5 minutes:
1. Find entered signals with missing outcomes (older than 60 min)
2. Check candle_store for outcome (did price hit ±0.5% target in next 10 candles?)
3. Update signal_log with outcome_pct and hit_target
4. Rebuild signal_stats aggregation table
"""

from __future__ import annotations

import asyncio

from app.modules.candle_store import query_candles
from app.modules.st_signal_logger import (
    get_pending_outcomes,
    rebuild_all_stats,
    update_outcome,
)
from app.utils.logging import get_logger

log = get_logger(__name__)

TARGET_PCT = 0.005  # %0.5
LOOKAHEAD_CANDLES = 10
UPDATE_INTERVAL = 300  # 5 minutes

_task: asyncio.Task[None] | None = None


async def _compute_outcome(signal: dict) -> tuple[float, bool] | None:
    """Compute outcome for a single signal by checking subsequent candles.

    Returns (outcome_pct, hit_target) or None if not enough data yet.
    """
    symbol = signal["symbol"]
    direction = signal["direction"]
    entry_price = signal["price"]
    signal_dt = signal["datetime"]

    # Fetch candles after signal time
    candles = await query_candles(
        symbol, "5m",
        limit=LOOKAHEAD_CANDLES + 5,
        date_from=signal_dt,
    )

    if len(candles) < LOOKAHEAD_CANDLES:
        return None  # Not enough subsequent data yet

    # Check if target was hit in the lookahead window
    # Skip candle[0] if it's the signal candle itself
    check_candles = candles[1:LOOKAHEAD_CANDLES + 1] if len(candles) > LOOKAHEAD_CANDLES else candles[1:]

    if not check_candles:
        return None

    hit_target = False
    best_pct = 0.0

    for c in check_candles:
        if direction == "BUY":
            pct = (c["high"] - entry_price) / entry_price
            if c["high"] >= entry_price * (1 + TARGET_PCT):
                hit_target = True
                best_pct = max(best_pct, pct)
                break
            best_pct = max(best_pct, pct)
        else:  # SELL
            pct = (entry_price - c["low"]) / entry_price
            if c["low"] <= entry_price * (1 - TARGET_PCT):
                hit_target = True
                best_pct = max(best_pct, pct)
                break
            best_pct = max(best_pct, pct)

    # If not hit, compute final outcome from last candle's close
    if not hit_target:
        last_close = check_candles[-1]["close"]
        if direction == "BUY":
            best_pct = (last_close - entry_price) / entry_price
        else:
            best_pct = (entry_price - last_close) / entry_price

    return round(best_pct * 100, 4), hit_target


async def _tick() -> None:
    """Single update cycle: compute outcomes + rebuild stats."""
    try:
        # 1. Find signals needing outcome computation
        pending = await get_pending_outcomes(lookback_minutes=60)

        updated = 0
        for signal in pending:
            result = await _compute_outcome(signal)
            if result is not None:
                outcome_pct, hit_target = result
                await update_outcome(signal["id"], outcome_pct, hit_target)
                updated += 1

        # 2. Rebuild aggregated stats
        stats_count = await rebuild_all_stats()

        if updated > 0 or stats_count > 0:
            log.info(
                "st_stats_updated",
                outcomes_computed=updated,
                stats_rows=stats_count,
            )

    except Exception as e:
        log.error("st_stats_tick_error", error=str(e))


async def _loop() -> None:
    """Main updater loop — runs until cancelled."""
    log.info("st_stats_updater_started", interval=UPDATE_INTERVAL)
    try:
        while True:
            await _tick()
            await asyncio.sleep(UPDATE_INTERVAL)
    except asyncio.CancelledError:
        log.info("st_stats_updater_stopped")


def start_st_stats_updater() -> asyncio.Task[None]:
    global _task
    _task = asyncio.create_task(_loop())
    return _task


async def stop_st_stats_updater() -> None:
    global _task
    if _task is not None and not _task.done():
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        _task = None
