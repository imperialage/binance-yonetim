"""Trade execution orchestration — per-symbol locking, position management."""

from __future__ import annotations

import asyncio
import time

from app.config import settings
from app.modules.binance_client import (
    BinanceAPIError,
    cancel_all_open_orders,
    cancel_order,
    get_exchange_info,
    get_order_status,
    get_position_risk,
    get_usdt_balance,
    place_limit_order,
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


def _clear_trade_pending(symbol: str) -> None:
    """trade_pending kilidini ac — islem acilamadi, yeni sinyal aranabilir."""
    try:
        from app.modules.signal_engine import get_engine
        eng = get_engine(symbol)
        if eng:
            eng.trade_pending = False
            eng.pending_order = None
    except Exception as e:
        log.warning("clear_trade_pending_failed", symbol=symbol, error=str(e))


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
            # trade_pending temizle — sıkışmasın
            try:
                from app.modules.signal_engine import get_engine
                eng = get_engine(symbol)
                if eng:
                    eng.trade_pending = False
                    eng.pending_order = None
            except Exception as exc:
                log.warning("clear_trade_pending_in_error_handler_failed", symbol=symbol, error=str(exc))
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
    except BinanceAPIError as e:
        # -4028: leverage not changed — that's fine
        if e.code != -4028:
            raise
    except Exception:
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

    # ── 2b. Sembol config — indicator_settings'ten oku (kalici) ──
    from app.modules.indicator_settings_store import get_settings_or_defaults
    sym_cfg = await get_settings_or_defaults(symbol)
    reverse_signal = bool(sym_cfg.get("reverse_signal", 0))
    sl_enabled = bool(sym_cfg.get("sl_enabled", 1))
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
        _clear_trade_pending(symbol)
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
            _clear_trade_pending(symbol)
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
                log.info("reverse_signal_closed", symbol=symbol, closed_side=close_side, qty=close_qty)
                await asyncio.sleep(0.5)  # Pozisyon guncellenmesi icin bekle
                # Fill dogrulamasi — pozisyon gercekten kapandi mi?
                verify_positions = await get_position_risk(symbol)
                for vp in verify_positions:
                    if vp.get("symbol") == symbol:
                        if float(vp.get("positionAmt", 0)) != 0:
                            log.error("reverse_close_not_filled", symbol=symbol, pos_amt=vp.get("positionAmt"))
                            duration = (time.monotonic_ns() // 1_000_000) - start_ms
                            await log_trade(
                                event_id=event_id, ts=ts, symbol=symbol, side=side,
                                status="FAILED", reason="Reverse close not filled",
                                duration_ms=duration,
                            )
                            return
                        break
                closed_previous = True
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
        _clear_trade_pending(symbol)
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
        _clear_trade_pending(symbol)
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
    sym_weight = sym_cfg.get("weight", 0.10)
    usable_balance = balance * sym_weight * 0.98
    raw_qty = usable_balance / price
    quantity = round_step_size(raw_qty, step_size)

    if quantity < min_qty:
        duration = (time.monotonic_ns() // 1_000_000) - start_ms
        _clear_trade_pending(symbol)
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

    # ── 8. Limit order ile giriş + anında SL ──
    ENTRY_BUFFER = 0.001  # binde 1
    LIMIT_TIMEOUT = 900   # 15 dakika (saniye)

    if side == "BUY":
        limit_price = round_price(price * (1 - ENTRY_BUFFER), tick_size)
    else:
        limit_price = round_price(price * (1 + ENTRY_BUFFER), tick_size)

    # SL fiyatını hesapla (indicator_settings'ten)
    ind_sl_pct = sym_cfg.get("sl_pct", 0.1) / 100.0  # yuzde → oran
    if side == "BUY":
        sl_price = round_price(limit_price * (1 - ind_sl_pct), tick_size)
        sl_side = "SELL"
    else:
        sl_price = round_price(limit_price * (1 + ind_sl_pct), tick_size)
        sl_side = "BUY"

    # Giriş emri ver
    order_result = await place_limit_order(symbol, side, quantity, limit_price)
    order_id = str(order_result.get("orderId", ""))

    log.info(
        "limit_order_placed",
        symbol=symbol,
        side=side,
        signal_price=price,
        limit_price=limit_price,
        quantity=quantity,
        order_id=order_id,
        timeout=LIMIT_TIMEOUT,
    )

    # HEMEN SL emri ver — giriş emriyle birlikte Binance'ta bekler
    sl_order_id = ""
    if sl_enabled:
        try:
            from app.modules.binance_client import place_stop_market_instant
            sl_result = await place_stop_market_instant(symbol, sl_side, quantity, sl_price)
            sl_order_id = str(sl_result.get("algoId", ""))
            log.info(
                "sl_instant_with_entry",
                symbol=symbol,
                sl_price=sl_price,
                sl_side=sl_side,
                sl_order_id=sl_order_id,
            )
        except Exception as e:
            log.error("sl_instant_failed", symbol=symbol, sl_price=sl_price, error=str(e))

    # ── 8b. Signal engine'e bilgi ver — fill takibi + TP oradan yapilacak ──
    try:
        from app.modules.signal_engine import get_engine
        engine = get_engine(symbol)
        if engine:
            # signal_id'yi event_id'den parse et (format: se-{row_id}-{ts})
            _sig_id = None
            _parts = event_id.split("-")
            if len(_parts) >= 2 and _parts[1].isdigit():
                _sig_id = int(_parts[1])
            engine.pending_order = {
                "order_id": order_id,
                "side": side,
                "limit_price": limit_price,
                "quantity": quantity,
                "event_id": event_id,
                "signal_id": _sig_id,
                "tick_size": tick_size,
                "sl_enabled": sl_enabled,
                "sl_order_id": sl_order_id,
                "sl_price": sl_price,
                "start_time": time.time(),
                "timeout": LIMIT_TIMEOUT,
                "signal_price": price,
                "balance": balance,
                "closed_previous": closed_previous,
            }
            engine.trade_pending = True
            if sl_order_id:
                engine.sl_confirmed = True
                engine.sl_price = sl_price
    except Exception:
        pass

    # execute_trade burada BITER
    # Fill takibi + TP koyma signal_engine.check_pending_fill() tarafindan yapilir
    return
