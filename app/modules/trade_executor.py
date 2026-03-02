"""Trade execution orchestration — per-symbol locking, position management."""

from __future__ import annotations

import asyncio
import time

from app.config import settings
from app.modules.binance_client import (
    cancel_all_open_orders,
    get_exchange_info,
    get_position_risk,
    get_usdt_balance,
    place_market_order,
    place_stop_market_order,
    round_price,
    round_step_size,
    set_leverage,
)
from app.modules.trade_store import log_trade
from app.utils.logging import get_logger

log = get_logger(__name__)

# Per-symbol locks to prevent race conditions
_locks: dict[str, asyncio.Lock] = {}


def _get_lock(symbol: str) -> asyncio.Lock:
    if symbol not in _locks:
        _locks[symbol] = asyncio.Lock()
    return _locks[symbol]


async def execute_trade(
    symbol: str,
    signal: str,
    price: float,
    event_id: str,
) -> None:
    """Execute a trade based on signal. Fire-and-forget from webhook."""
    start_ms = time.monotonic_ns() // 1_000_000
    ts = int(time.time())
    side = "BUY" if signal == "BUY" else "SELL"

    # ── Kill-switch check ────────────────────────────
    if not settings.trading_enabled:
        log.info("trading_disabled_skip", symbol=symbol, signal=signal)
        return

    lock = _get_lock(symbol)
    async with lock:
        try:
            await _execute_trade_inner(symbol, side, price, event_id, ts, start_ms)
        except Exception as e:
            duration = (time.monotonic_ns() // 1_000_000) - start_ms
            log.error(
                "trade_execution_error",
                symbol=symbol,
                side=side,
                event_id=event_id,
                error=str(e),
            )
            await log_trade(
                event_id=event_id,
                ts=ts,
                symbol=symbol,
                side=side,
                status="ERROR",
                reason=str(e),
                duration_ms=duration,
            )


async def _execute_trade_inner(
    symbol: str,
    side: str,
    price: float,
    event_id: str,
    ts: int,
    start_ms: int,
) -> None:
    """Inner trade logic — called under per-symbol lock."""

    # ── 1. Set leverage to 1x ────────────────────────
    try:
        await set_leverage(symbol, leverage=1)
    except Exception as e:
        # -4028: leverage not changed — that's fine
        if "-4028" not in str(e):
            raise

    # ── 2. Check current position ────────────────────
    positions = await get_position_risk(symbol)
    current_pos = None
    for p in positions:
        if p.get("symbol") == symbol:
            current_pos = p
            break

    pos_amt = float(current_pos.get("positionAmt", 0)) if current_pos else 0.0
    closed_previous = False

    # ── 3. Same direction check ──────────────────────
    if (side == "BUY" and pos_amt > 0) or (side == "SELL" and pos_amt < 0):
        duration = (time.monotonic_ns() // 1_000_000) - start_ms
        log.info("same_direction_position_exists", symbol=symbol, side=side, pos_amt=pos_amt)
        await log_trade(
            event_id=event_id,
            ts=ts,
            symbol=symbol,
            side=side,
            status="SKIPPED",
            reason=f"Same direction position already exists: {pos_amt}",
            duration_ms=duration,
        )
        return

    # ── 4. Close opposite position if exists ─────────
    if pos_amt != 0:
        log.info("closing_opposite_position", symbol=symbol, side=side, pos_amt=pos_amt)
        await cancel_all_open_orders(symbol)
        close_side = "SELL" if pos_amt > 0 else "BUY"
        close_qty = abs(pos_amt)
        await place_market_order(symbol, close_side, close_qty, reduce_only=True)
        closed_previous = True
        log.info("opposite_position_closed", symbol=symbol, close_side=close_side, qty=close_qty)

    # ── 5. Get exchange info for rounding ────────────
    info = await get_exchange_info(symbol)
    step_size = info["lotSize"]["stepSize"]
    min_qty = info["lotSize"]["minQty"]
    tick_size = info["priceFilter"]["tickSize"]
    price_precision = info["pricePrecision"]

    # ── 6. Get available balance ─────────────────────
    balance = await get_usdt_balance()
    if balance <= 0:
        duration = (time.monotonic_ns() // 1_000_000) - start_ms
        await log_trade(
            event_id=event_id,
            ts=ts,
            symbol=symbol,
            side=side,
            status="FAILED",
            reason=f"Insufficient balance: {balance}",
            duration_ms=duration,
        )
        return

    # ── 7. Calculate quantity ────────────────────────
    if price <= 0:
        duration = (time.monotonic_ns() // 1_000_000) - start_ms
        await log_trade(
            event_id=event_id,
            ts=ts,
            symbol=symbol,
            side=side,
            status="FAILED",
            reason=f"Invalid price: {price}",
            duration_ms=duration,
        )
        return

    raw_qty = balance / price
    quantity = round_step_size(raw_qty, step_size)

    if quantity < min_qty:
        duration = (time.monotonic_ns() // 1_000_000) - start_ms
        await log_trade(
            event_id=event_id,
            ts=ts,
            symbol=symbol,
            side=side,
            status="FAILED",
            reason=f"Quantity {quantity} below minimum {min_qty}",
            balance_used=balance,
            duration_ms=duration,
        )
        return

    # ── 8. Place market order ────────────────────────
    order_result = await place_market_order(symbol, side, quantity)
    order_id = str(order_result.get("orderId", ""))

    # Use avgPrice from order result if available, else use signal price
    entry_price = float(order_result.get("avgPrice", 0))
    if entry_price <= 0:
        entry_price = price

    log.info(
        "market_order_filled",
        symbol=symbol,
        side=side,
        quantity=quantity,
        entry_price=entry_price,
        order_id=order_id,
    )

    # ── 9. Place stop-loss ───────────────────────────
    sl_pct = settings.stop_loss_pct
    if side == "BUY":
        # Long position: stop-loss below entry
        raw_stop = entry_price * (1 - sl_pct)
        stop_side = "SELL"
    else:
        # Short position: stop-loss above entry
        raw_stop = entry_price * (1 + sl_pct)
        stop_side = "BUY"

    stop_price = round_price(raw_stop, tick_size)

    stop_order_id = None
    try:
        stop_result = await place_stop_market_order(symbol, stop_side, quantity, stop_price)
        stop_order_id = str(stop_result.get("algoId", stop_result.get("orderId", "")))
        log.info(
            "stop_loss_placed",
            symbol=symbol,
            stop_side=stop_side,
            stop_price=stop_price,
            stop_order_id=stop_order_id,
        )
    except Exception as e:
        log.error("stop_loss_failed", symbol=symbol, error=str(e))

    # ── 10. Log trade ────────────────────────────────
    duration = (time.monotonic_ns() // 1_000_000) - start_ms
    await log_trade(
        event_id=event_id,
        ts=ts,
        symbol=symbol,
        side=side,
        quantity=quantity,
        entry_price=entry_price,
        stop_price=stop_price,
        order_id=order_id,
        stop_order_id=stop_order_id,
        status="FILLED",
        closed_previous=closed_previous,
        balance_used=balance,
        duration_ms=duration,
    )
