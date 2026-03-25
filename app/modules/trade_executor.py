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
    tp_pct: float | None = None,
    sl_pct: float | None = None,
) -> None:
    """Execute a trade based on signal. Fire-and-forget from webhook.

    tp_pct/sl_pct: per-symbol overrides. If None, falls back to settings.get_strategy(tf).
    """
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
            await _execute_trade_inner(symbol, side, price, event_id, ts, start_ms, tf, tp_pct=tp_pct, sl_pct=sl_pct)
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
    tp_pct: float | None = None,
    sl_pct: float | None = None,
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

    # ── 2b. Sembol config (sl_enabled, reverse_signal) ──
    from app.config import get_symbol_config as _get_sym_cfg
    sym_cfg = _get_sym_cfg(symbol)
    reverse_signal = sym_cfg.get("reverse_signal", False)
    sl_enabled = sym_cfg.get("sl_enabled", True)
    # reverse_signal acikken SL konmaz
    if reverse_signal:
        sl_enabled = False

    # ── 2c. Clean orphan orders if no position ──────
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

    # ── 4. Opposite position check ──────────────────
    if pos_amt != 0:
        if not reverse_signal:
            # reverse_signal kapali → ters sinyal skip, TP/SL ile kapanmasini bekle
            duration = (time.monotonic_ns() // 1_000_000) - start_ms
            log.info("opposite_position_exists_skip", symbol=symbol, side=side, pos_amt=pos_amt)
            await log_trade(
                event_id=event_id,
                ts=ts,
                symbol=symbol,
                side=side,
                status="SKIPPED",
                reason=f"Opposite position exists ({pos_amt}), waiting for TP/SL",
                duration_ms=duration,
            )
            return
        else:
            # reverse_signal acik → mevcut pozisyonu kapat + yeni ac
            log.info("reverse_signal_closing", symbol=symbol, side=side, pos_amt=pos_amt)
            try:
                await cancel_all_open_orders(symbol)
                close_side = "SELL" if pos_amt > 0 else "BUY"
                close_qty = abs(pos_amt)
                await place_market_order(symbol, close_side, close_qty, reduce_only=True)
                closed_previous = True
                log.info("reverse_signal_closed", symbol=symbol, closed_side=close_side, qty=close_qty)
                await asyncio.sleep(0.5)  # Pozisyon guncellenmesi icin bekle
            except Exception as e:
                duration = (time.monotonic_ns() // 1_000_000) - start_ms
                log.error("reverse_signal_close_failed", symbol=symbol, error=str(e))
                await log_trade(
                    event_id=event_id,
                    ts=ts,
                    symbol=symbol,
                    side=side,
                    status="FAILED",
                    reason=f"Failed to close opposite position: {e}",
                    duration_ms=duration,
                )
                return

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

    # EV-weighted capital allocation — her sembol kendi ağırlığı kadar bakiye kullanır
    from app.config import get_symbol_config
    sym_weight = get_symbol_config(symbol).get("weight", 1.0 / 6)
    usable_balance = balance * sym_weight * 0.98
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

    # ── 8. Market order ile giriş (hızlı, tüm fiyat seviyelerinde çalışır) ──
    order_result = await place_market_order(symbol, side, quantity)
    order_id = str(order_result.get("orderId", ""))

    # Gerçek giriş fiyatını al (market order avgPrice=0 dönebilir)
    await asyncio.sleep(0.3)
    pos_data = await get_position_risk(symbol)
    entry_price = 0.0
    for p in pos_data:
        if p.get("symbol") == symbol:
            entry_price = float(p.get("entryPrice", 0))
            break
    if entry_price <= 0:
        entry_price = float(order_result.get("avgPrice", 0))
    if entry_price <= 0:
        entry_price = price

    log.info(
        "market_order_filled",
        symbol=symbol,
        side=side,
        tf=tf,
        quantity=quantity,
        entry_price=entry_price,
        signal_price=price,
        order_id=order_id,
    )

    # ── 9. Place TP + SL ────────────────────────────────
    _default_sl, _default_tp = settings.get_strategy(tf)
    if tp_pct is None:
        tp_pct = _default_tp
    if sl_pct is None:
        sl_pct = _default_sl

    if side == "BUY":
        raw_tp = entry_price * (1 + tp_pct)
        raw_sl = entry_price * (1 - sl_pct)
        exit_side = "SELL"
    else:
        raw_tp = entry_price * (1 - tp_pct)
        raw_sl = entry_price * (1 + sl_pct)
        exit_side = "BUY"

    tp_price = round_price(raw_tp, tick_size)
    sl_price = round_price(raw_sl, tick_size)

    # TP her zaman konur
    tp_order_id = None
    try:
        tp_result = await place_take_profit_market_order(symbol, exit_side, quantity, tp_price)
        tp_order_id = str(tp_result.get("algoId", tp_result.get("orderId", "")))
        log.info("take_profit_placed", symbol=symbol, tp_price=tp_price)
    except Exception as e:
        log.error("take_profit_failed", symbol=symbol, error=str(e))

    # SL sadece sl_enabled ise konur (reverse_signal acikken sl_enabled=False)
    sl_order_id = None
    if sl_enabled:
        try:
            sl_result = await place_stop_market_order(symbol, exit_side, quantity, sl_price)
            sl_order_id = str(sl_result.get("algoId", sl_result.get("orderId", "")))
            log.info("stop_loss_placed", symbol=symbol, sl_price=sl_price)
        except Exception as e:
            log.error("stop_loss_failed", symbol=symbol, error=str(e))
    else:
        log.info("stop_loss_skipped", symbol=symbol, sl_enabled=False, reverse_signal=reverse_signal)
        sl_price = 0.0

    # ── 10. Log trade ────────────────────────────────
    duration = (time.monotonic_ns() // 1_000_000) - start_ms
    await log_trade(
        event_id=event_id,
        ts=ts,
        symbol=symbol,
        side=side,
        quantity=quantity,
        entry_price=entry_price,
        stop_price=sl_price if sl_enabled else None,
        order_id=order_id,
        stop_order_id=sl_order_id,
        status="FILLED",
        closed_previous=closed_previous,
        balance_used=balance,
        duration_ms=duration,
    )
