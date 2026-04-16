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
    get_total_wallet_balance,
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
        from app.modules.ha_signal_engine import get_ha_engine
        eng = get_ha_engine(symbol) or get_engine(symbol)
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
    prefetch: asyncio.Task | None = None,
    webhook_tp: float | None = None,
    webhook_sl: float | None = None,
) -> None:
    """Execute a trade based on signal. Fire-and-forget from webhook.

    prefetch: pre-fetched (positions, balance) asyncio.Task — sinyal aninda baslatilir.
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
            await _execute_trade_inner(symbol, side, price, event_id, ts, start_ms, tf, tp_pct=tp_pct, sl_pct=sl_pct, prefetch=prefetch, webhook_tp=webhook_tp, webhook_sl=webhook_sl)
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
                from app.modules.ha_signal_engine import get_ha_engine
                eng = get_ha_engine(symbol) or get_engine(symbol)
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


# Exchange info cache — startup'ta doldurulur, bellekten okunur
_exchange_cache: dict[str, dict] = {}


async def get_exchange_info_cached(symbol: str) -> dict:
    """Exchange info cache — step_size, tick_size ayda bir degisir."""
    if symbol not in _exchange_cache:
        _exchange_cache[symbol] = await get_exchange_info(symbol)
    return _exchange_cache[symbol]


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
    prefetch: asyncio.Task | None = None,
    webhook_tp: float | None = None,
    webhook_sl: float | None = None,
) -> None:
    """Inner trade logic — called under per-symbol lock."""

    # ── 1. Leverage: startup'ta set edildi, burada atla ──

    # ── 2. Position + Balance: pre-fetch varsa kullan, yoksa paralel cek ──
    # balance = TOPLAM cuzdan bakiyesi (acik pozisyonlarin margin'i dahil)
    # Weight hesabi toplam bakiye uzerinden → her sembol bagimsiz alan alir
    if prefetch:
        try:
            positions, balance = await prefetch
        except Exception:
            positions, balance = await asyncio.gather(
                get_position_risk(symbol), get_total_wallet_balance(),
            )
    else:
        positions, balance = await asyncio.gather(
            get_position_risk(symbol), get_total_wallet_balance(),
        )

    # Kullanilabilir bakiye (margin acigi icin)
    try:
        available_balance = await get_usdt_balance()
    except Exception:
        available_balance = balance

    current_pos = None
    for p in positions:
        if p.get("symbol") == symbol:
            current_pos = p
            break

    pos_amt = float(current_pos.get("positionAmt", 0)) if current_pos else 0.0
    closed_previous = False

    # ── 2b. Sembol config — indicator_settings'ten oku (lokal DB, <1ms) ──
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
    info = await get_exchange_info_cached(symbol)
    step_size = info["lotSize"]["stepSize"]
    min_qty = info["lotSize"]["minQty"]
    tick_size = info["priceFilter"]["tickSize"]
    price_precision = info["pricePrecision"]

    # ── 6. Balance kontrolu (paralel call'dan geldi) ─
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

    # Weight hesabi — TOPLAM cuzdan bakiyesi uzerinden (acik pozisyonlardan bagimsiz)
    # Boylece %20 MYX + %50 ETH = toplam %70, her biri kendi dilimini alir
    sym_weight = sym_cfg.get("weight", 0.10)
    usable_balance = balance * sym_weight * 0.98

    # Margin yetersizlik uyarisi
    if usable_balance > available_balance:
        log.warning(
            "insufficient_available_balance",
            symbol=symbol, side=side,
            total_balance=balance, available=available_balance,
            required=usable_balance, weight=sym_weight,
            msg="Weight toplami acik pozisyonlardan dolayi bakiyeyi asmis olabilir",
        )

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

    # ── 8. Giriş emri + SL ──
    use_market = bool(sym_cfg.get("market_entry", 0))
    LIMIT_TIMEOUT = 900   # 15 dakika (saniye)

    entry_price = round_price(price, tick_size)

    # SL fiyati SADECE webhook'tan (Pine Script) — baska kaynaktan gelmez
    sl_price_calc = round_price(webhook_sl, tick_size) if webhook_sl else None
    sl_side = "SELL" if side == "BUY" else "BUY"
    if not sl_price_calc:
        sl_enabled = False  # webhook'tan SL gelmezse SL emri VERILMEZ

    # Giriş emri ver — MARKET veya LIMIT
    if use_market:
        order_result = await place_market_order(symbol, side, quantity)
        order_id = str(order_result.get("orderId", ""))
        fill_price = float(order_result.get("avgPrice", 0)) or entry_price
        # SL: webhook fiyati korunur — fill price'dan yeniden hesaplanmaz
        log.info("market_order_placed", symbol=symbol, side=side, qty=quantity,
                 fill_price=fill_price, order_id=order_id)
    else:
        order_result = await place_limit_order(symbol, side, quantity, entry_price)
        order_id = str(order_result.get("orderId", ""))
        fill_price = entry_price
        log.info("limit_order_placed", symbol=symbol, side=side, signal_price=price,
                 limit_price=entry_price, quantity=quantity, order_id=order_id, timeout=LIMIT_TIMEOUT)

    # SL emri ver
    sl_order_id = ""
    if sl_enabled:
        try:
            from app.modules.binance_client import place_stop_market_instant
            sl_result = await place_stop_market_instant(symbol, sl_side, quantity, sl_price_calc)
            sl_order_id = str(sl_result.get("algoId", ""))
            log.info("sl_with_entry", symbol=symbol, sl_price=sl_price_calc, sl_side=sl_side, sl_order_id=sl_order_id)
        except Exception as e:
            log.error("sl_instant_failed", symbol=symbol, sl_price=sl_price_calc, error=str(e))

    # ── 8b. Signal engine'e bilgi ver — fill takibi + TP oradan yapilacak ──
    try:
        from app.modules.signal_engine import get_engine
        from app.modules.ha_signal_engine import get_ha_engine
        engine = get_ha_engine(symbol) or get_engine(symbol)
        if engine:
            _sig_id = None
            _parts = event_id.split("-")
            if len(_parts) >= 2 and _parts[1].isdigit():
                _sig_id = int(_parts[1])
            engine.pending_order = {
                "order_id": order_id,
                "side": side,
                "limit_price": fill_price,
                "quantity": quantity,
                "event_id": event_id,
                "signal_id": _sig_id,
                "tick_size": tick_size,
                "sl_enabled": sl_enabled,
                "sl_order_id": sl_order_id,
                "sl_price": sl_price_calc,
                "start_time": time.time(),
                "timeout": LIMIT_TIMEOUT,
                "signal_price": price,
                "balance": balance,
                "closed_previous": closed_previous,
                "webhook_tp_price": webhook_tp,
                "webhook_sl_price": webhook_sl,
            }
            engine.trade_pending = True
            if sl_order_id:
                engine.sl_confirmed = True
                engine.sl_price = sl_price_calc
            # Bar-close validation — mum kapanisinda sinyal dogrulama icin
            from app.modules.signal_engine import INTERVAL_SECONDS
            iv_sec = INTERVAL_SECONDS.get(tf, 300)
            engine.webhook_entry_bar_time = (int(time.time()) // iv_sec) * iv_sec
            engine.webhook_entry_direction = side
    except Exception:
        pass

    return
