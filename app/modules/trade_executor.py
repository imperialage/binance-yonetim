"""Trade execution orchestration — per-symbol locking, position management."""

from __future__ import annotations

import asyncio
import time

from app.config import settings
from app.modules.binance_client import (
    cancel_all_open_orders,
    cancel_order,
    get_exchange_info,
    get_order_status,
    get_position_risk,
    get_usdt_balance,
    place_limit_order,
    place_market_order,
    place_take_profit_market_order,
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
    tf: str = "5m",
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
            await _execute_trade_inner(symbol, side, price, event_id, ts, start_ms, tf)
        except Exception as e:
            duration = (time.monotonic_ns() // 1_000_000) - start_ms
            log.error(
                "trade_execution_error",
                symbol=symbol,
                side=side,
                tf=tf,
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
    tf: str = "5m",
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

    # ── 2b. Clean orphan orders if no position ──────
    if pos_amt == 0:
        try:
            await cancel_all_open_orders(symbol)
            log.info("orphan_orders_cleaned", symbol=symbol)
        except Exception:
            pass

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
        # Wait for balance settlement after closing position
        await asyncio.sleep(0.5)

    # ── 5. Get exchange info for rounding ────────────
    info = await get_exchange_info(symbol)
    step_size = info["lotSize"]["stepSize"]
    min_qty = info["lotSize"]["minQty"]
    tick_size = info["priceFilter"]["tickSize"]
    price_precision = info["pricePrecision"]

    # ── 6. Get available balance (fresh after close) ─
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

    # Use 98% of balance to leave room for fees/commission
    usable_balance = balance * 0.98
    raw_qty = usable_balance / price
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

    # ── 8. Place limit order (LONG: sinyal-$1, SHORT: sinyal+$1) ──
    LIMIT_OFFSET = 1.0  # $1 offset
    if side == "BUY":
        limit_price = round_price(price - LIMIT_OFFSET, tick_size)
    else:
        limit_price = round_price(price + LIMIT_OFFSET, tick_size)

    order_result = await place_limit_order(symbol, side, quantity, limit_price)
    order_id_int = int(order_result.get("orderId", 0))
    order_id = str(order_id_int)

    log.info(
        "limit_order_placed",
        symbol=symbol,
        side=side,
        tf=tf,
        quantity=quantity,
        limit_price=limit_price,
        signal_price=price,
        order_id=order_id,
    )

    # Limit order dolmasını bekle (max 60 saniye, her 1sn kontrol)
    FILL_TIMEOUT = 60
    filled = False
    entry_price = limit_price
    for _ in range(FILL_TIMEOUT):
        await asyncio.sleep(1)
        try:
            status = await get_order_status(symbol, order_id_int)
            order_status = status.get("status", "")
            if order_status == "FILLED":
                entry_price = float(status.get("avgPrice", 0)) or limit_price
                filled = True
                break
            elif order_status in ("CANCELED", "EXPIRED", "REJECTED"):
                break
        except Exception:
            pass

    if not filled:
        # Dolmadı — iptal et ve market order ile gir
        try:
            await cancel_order(symbol, order_id_int)
            log.info("limit_order_cancelled_timeout", symbol=symbol, order_id=order_id)
        except Exception:
            pass  # Zaten dolmuş veya iptal olmuş olabilir

        # Market order fallback
        order_result = await place_market_order(symbol, side, quantity)
        order_id = str(order_result.get("orderId", ""))
        entry_price = float(order_result.get("avgPrice", 0))
        if entry_price <= 0:
            entry_price = price
        log.info(
            "market_order_fallback",
            symbol=symbol,
            side=side,
            quantity=quantity,
            entry_price=entry_price,
        )
    else:
        log.info(
            "limit_order_filled",
            symbol=symbol,
            side=side,
            quantity=quantity,
            entry_price=entry_price,
        )

    # ── 9. Place take-profit only (SL yok — ters sinyal ile kapanır) ──
    _sl_pct, tp_pct = settings.get_strategy(tf)
    if side == "BUY":
        raw_tp = entry_price * (1 + tp_pct)
        tp_side = "SELL"
    else:
        raw_tp = entry_price * (1 - tp_pct)
        tp_side = "BUY"

    tp_price = round_price(raw_tp, tick_size)

    tp_order_id = None
    try:
        tp_result = await place_take_profit_market_order(symbol, tp_side, quantity, tp_price)
        tp_order_id = str(tp_result.get("algoId", tp_result.get("orderId", "")))
        log.info(
            "take_profit_placed",
            symbol=symbol,
            tp_side=tp_side,
            tp_price=tp_price,
            tp_order_id=tp_order_id,
        )
    except Exception as e:
        log.error("take_profit_failed", symbol=symbol, error=str(e))

    # ── 10. Log trade ────────────────────────────────
    duration = (time.monotonic_ns() // 1_000_000) - start_ms
    await log_trade(
        event_id=event_id,
        ts=ts,
        symbol=symbol,
        side=side,
        quantity=quantity,
        entry_price=entry_price,
        stop_price=0.0,
        order_id=order_id,
        stop_order_id=None,
        status="FILLED",
        closed_previous=closed_previous,
        balance_used=balance,
        duration_ms=duration,
    )
