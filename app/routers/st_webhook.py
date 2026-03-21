"""POST /st-webhook – SuperTrend signal receiver with data-driven filters."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.modules.st_entry_optimizer import check_entry
from app.modules.st_filter_engine import run_filters
from app.modules.st_signal_logger import log_st_signal
from app.modules.trade_executor import execute_trade
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()


class STWebhookPayload(BaseModel):
    """SuperTrend webhook payload from TradingView."""

    symbol: str = Field(examples=["ETHUSDT"])
    direction: str = Field(description="BUY or SELL")
    price: float
    band: str = Field(description="HIGH, MID, or LOW")
    time: str = Field(description="ISO datetime e.g. 2026-03-18T23:10:00Z")


@router.post("/st-webhook")
async def st_webhook(payload: STWebhookPayload) -> JSONResponse:
    symbol = payload.symbol.strip().upper()
    direction = payload.direction.strip().upper()
    band = payload.band.strip().upper()
    price = payload.price

    if direction not in ("BUY", "SELL"):
        return JSONResponse(
            status_code=400,
            content={"detail": f"Invalid direction: {direction}. Must be BUY or SELL."},
        )

    # Parse signal time
    try:
        signal_dt = datetime.fromisoformat(payload.time.replace("Z", "+00:00"))
    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"detail": f"Invalid time format: {payload.time}"},
        )

    signal_ts = int(signal_dt.timestamp())
    dt_str = signal_dt.strftime("%Y-%m-%d %H:%M:%S")

    log.info(
        "st_webhook_received",
        symbol=symbol,
        direction=direction,
        band=band,
        price=price,
        time=dt_str,
    )

    # ── 1. Run filters ────────────────────────────────────
    passed, filter_results, vol_ratio = await run_filters(
        symbol=symbol,
        direction=direction,
        band=band,
        price=price,
        signal_time=signal_dt,
    )

    if not passed:
        # Find the failing filter
        failed = next((f for f in filter_results if not f["pass"]), None)
        skip_filter = failed["name"] if failed else "unknown"
        skip_reason = failed["reason"] if failed else "unknown"

        log.info(
            "st_signal_filtered",
            symbol=symbol,
            direction=direction,
            filter=skip_filter,
            reason=skip_reason,
        )

        row_id = await log_st_signal(
            dt=dt_str,
            symbol=symbol,
            direction=direction,
            band=band,
            price=price,
            vol_ratio=vol_ratio,
            entered=False,
            skip_filter=skip_filter,
            skip_reason=skip_reason,
        )

        return JSONResponse(content={
            "status": "filtered",
            "signal_id": row_id,
            "filter": skip_filter,
            "reason": skip_reason,
            "filters": filter_results,
        })

    # ── 2. Entry optimization ─────────────────────────────
    entry_result = await check_entry(
        symbol=symbol,
        direction=direction,
        price=price,
        signal_time=signal_ts,
    )

    if not entry_result.passed:
        log.info(
            "st_signal_entry_rejected",
            symbol=symbol,
            direction=direction,
            reason=entry_result.reason,
        )

        row_id = await log_st_signal(
            dt=dt_str,
            symbol=symbol,
            direction=direction,
            band=band,
            price=price,
            vol_ratio=vol_ratio,
            entered=False,
            skip_filter="entry_optimizer",
            skip_reason=entry_result.reason,
        )

        return JSONResponse(content={
            "status": "entry_rejected",
            "signal_id": row_id,
            "reason": entry_result.reason,
            "checks": entry_result.checks,
            "filters": filter_results,
        })

    # ── 3. Log as entered ─────────────────────────────────
    row_id = await log_st_signal(
        dt=dt_str,
        symbol=symbol,
        direction=direction,
        band=band,
        price=price,
        vol_ratio=vol_ratio,
        entered=True,
    )

    # ── 4. Execute trade (if enabled) ─────────────────────
    trade_dispatched = False
    if settings.trading_enabled:
        event_id = f"st-{row_id}-{signal_ts}"
        tf = settings.trading_timeframes.split(",")[0].strip() or "5m"
        asyncio.create_task(execute_trade(
            symbol=symbol,
            signal=direction,
            price=price,
            event_id=event_id,
            tf=tf,
        ))
        trade_dispatched = True
        log.info("st_trade_dispatched", symbol=symbol, direction=direction, price=price)
    else:
        log.info("st_trade_skipped_disabled", symbol=symbol, direction=direction)

    return JSONResponse(content={
        "status": "accepted",
        "signal_id": row_id,
        "direction": direction,
        "band": band,
        "price": price,
        "vol_ratio": vol_ratio,
        "trade_dispatched": trade_dispatched,
        "trading_enabled": settings.trading_enabled,
        "filters": filter_results,
        "entry_checks": entry_result.checks,
    })
