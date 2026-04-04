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
    sym_weight = sym_cfg.get("weight", 0.10)
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

    # ── 8. Limit order ile giriş (binde 1 buffer, 15dk timeout) ──
    ENTRY_BUFFER = 0.001  # binde 1
    LIMIT_TIMEOUT = 900   # 15 dakika (saniye)
    POLL_INTERVAL = 5     # 5 saniye polling

    if side == "BUY":
        limit_price = round_price(price * (1 - ENTRY_BUFFER), tick_size)
    else:
        limit_price = round_price(price * (1 + ENTRY_BUFFER), tick_size)

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

    # ── 8b. Fill callback kaydet — order_stream FILLED event'inde TP/SL konacak ──
    _pending_trades[symbol] = {
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "limit_price": limit_price,
        "order_id": order_id,
        "event_id": event_id,
        "tf": tf,
        "tick_size": tick_size,
        "sl_enabled": sl_enabled,
        "reverse_signal": reverse_signal,
        "start_ms": start_ms,
        "closed_previous": closed_previous,
        "balance": balance,
        "signal_price": price,
    }

    from app.modules.order_stream import register_fill_callback
    register_fill_callback(symbol, _on_entry_fill)

    # ── 8c. Timeout gorevi — 15dk sonra dolmadiysa iptal ──
    asyncio.create_task(_timeout_watcher(symbol, order_id, LIMIT_TIMEOUT))

    # execute_trade burada BITER — TP/SL callback'te konacak
    return


# ── Pending trades — fill callback icin bilgi deposu ──
_pending_trades: dict[str, dict] = {}


async def _timeout_watcher(symbol: str, order_id: str, timeout: int) -> None:
    """Limit order timeout — dolmadiysa iptal et."""
    await asyncio.sleep(timeout)

    # Hala pending mi?
    pending = _pending_trades.pop(symbol, None)
    if not pending:
        return  # callback zaten isledi, sorun yok

    from app.modules.order_stream import unregister_fill_callback
    unregister_fill_callback(symbol)

    # Emri iptal et
    try:
        await cancel_order(symbol, int(order_id))
        log.info("limit_order_timeout_cancelled", symbol=symbol, order_id=order_id)
    except Exception as e:
        log.warning("limit_cancel_error", symbol=symbol, error=str(e))

    # signal_engine'e trade_pending temizle
    try:
        from app.modules.signal_engine import get_engine
        engine = get_engine(symbol)
        if engine:
            engine.trade_pending = False
    except Exception:
        pass

    await log_trade(
        event_id=pending.get("event_id", ""),
        ts=int(time.time()),
        symbol=symbol,
        side=pending.get("side", ""),
        quantity=pending.get("quantity", 0),
        entry_price=pending.get("limit_price", 0),
        order_id=order_id,
        status="TIMEOUT",
        reason=f"Limit order not filled in {timeout}s, cancelled",
    )

    # Sinyal kaydini guncelle — "fiyat olusmadi" notu
    try:
        from app.modules.st_signal_logger import get_db as get_signal_db
        db = await get_signal_db()
        event_id = pending.get("event_id", "")
        # event_id = "se-{row_id}-{timestamp}" formatinda
        parts = event_id.split("-")
        if len(parts) >= 2:
            row_id = int(parts[1])
            await db.execute(
                "UPDATE signal_log SET skip_reason = ?, entered = 0 WHERE id = ?",
                ("Fiyat olusmadi - 15dk timeout", row_id),
            )
            await db.commit()
    except Exception:
        pass


async def _on_entry_fill(event_data: dict) -> None:
    """order_stream'den FILLED event geldi — HEMEN TP + SL koy."""
    symbol = event_data.get("symbol", "")
    pending = _pending_trades.pop(symbol, None)
    if not pending:
        return

    from app.modules.order_stream import unregister_fill_callback
    unregister_fill_callback(symbol)

    avg_price = float(event_data.get("avg_price", 0))
    filled_qty = float(event_data.get("qty", 0)) or pending["quantity"]
    side = pending["side"]
    tick_size = pending["tick_size"]
    sl_enabled = pending["sl_enabled"]
    event_id = pending["event_id"]
    tf = pending["tf"]
    start_ms = pending["start_ms"]

    # Gercek giris fiyatini pozisyondan dogrula
    entry_price = avg_price or pending["limit_price"]
    try:
        await asyncio.sleep(0.2)
        pos_data = await get_position_risk(symbol)
        for p in pos_data:
            if p.get("symbol") == symbol:
                real_entry = float(p.get("entryPrice", 0))
                if real_entry > 0:
                    entry_price = real_entry
                filled_qty = abs(float(p.get("positionAmt", 0))) or filled_qty
                break
    except Exception:
        pass

    log.info(
        "trade_entry_confirmed",
        symbol=symbol,
        side=side,
        tf=tf,
        quantity=filled_qty,
        entry_price=entry_price,
        signal_price=pending["signal_price"],
        limit_price=pending["limit_price"],
        order_id=pending["order_id"],
    )

    # ── TP/SL hesapla ──
    from app.modules.indicator_settings_store import get_settings_or_defaults
    sym_cfg = await get_settings_or_defaults(symbol)
    tp_pct = sym_cfg.get("tp_pct", 1.0) / 100.0
    sl_pct = sym_cfg.get("sl_pct", 0.3) / 100.0

    if side == "BUY":
        tp_price = round_price(entry_price * (1 + tp_pct), tick_size)
        sl_price = round_price(entry_price * (1 - sl_pct), tick_size)
        exit_side = "SELL"
    else:
        tp_price = round_price(entry_price * (1 - tp_pct), tick_size)
        sl_price = round_price(entry_price * (1 + sl_pct), tick_size)
        exit_side = "BUY"

    # ── TP koy (retry ile) ──
    tp_order_id = None
    for attempt in range(3):
        try:
            tp_result = await place_take_profit_market_order(symbol, exit_side, filled_qty, tp_price)
            tp_order_id = str(tp_result.get("algoId", tp_result.get("orderId", "")))
            log.info("take_profit_placed", symbol=symbol, tp_price=tp_price, attempt=attempt)
            break
        except Exception as e:
            log.error("take_profit_failed", symbol=symbol, tp_price=tp_price, attempt=attempt, error=str(e))
            if attempt < 2:
                await asyncio.sleep(1)

    # ── SL koy (retry ile) ──
    sl_order_id = None
    if sl_enabled:
        for attempt in range(3):
            try:
                sl_result = await place_stop_market_order(symbol, exit_side, filled_qty, sl_price)
                sl_order_id = str(sl_result.get("algoId", sl_result.get("orderId", "")))
                log.info("stop_loss_placed", symbol=symbol, sl_price=sl_price, attempt=attempt)
                break
            except Exception as e:
                log.error("stop_loss_failed", symbol=symbol, sl_price=sl_price, attempt=attempt, error=str(e))
                if attempt < 2:
                    await asyncio.sleep(1)
    else:
        sl_price = 0.0

    # ── Log trade ──
    duration = (time.monotonic_ns() // 1_000_000) - start_ms
    await log_trade(
        event_id=event_id,
        ts=int(time.time()),
        symbol=symbol,
        side=side,
        quantity=filled_qty,
        entry_price=entry_price,
        stop_price=sl_price if sl_enabled else None,
        order_id=pending["order_id"],
        stop_order_id=sl_order_id,
        status="FILLED",
        closed_previous=pending["closed_previous"],
        balance_used=pending["balance"],
        duration_ms=duration,
    )
