"""Divergence trade executor — limit order giris + WebSocket fill → TP/SL.

Hidden Divergence sinyallerinde:
1. Limit order koy (GTC)
2. WebSocket USER_DATA ile fill bekle
3. Fill olunca → asyncio.gather(TP, SL) paralel koy
"""

from __future__ import annotations

import asyncio
import time

from app.config import settings, get_symbol_config
from app.modules.binance_client import (
    cancel_all_open_orders,
    get_exchange_info,
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
from app.modules.order_stream import register_fill_callback
from app.modules.trade_store import log_trade
from app.utils.logging import get_logger

log = get_logger(__name__)

# Bekleyen limit order takibi: symbol → {order_id, side, entry_price, ...}
_pending_orders: dict[str, dict] = {}


async def execute_divergence_trade(
    symbol: str,
    direction: str,
    entry_price: float,
    event_id: str,
    signal: object,
    cfg: dict,
) -> None:
    """Divergence sinyali ile limit order ac, fill'de TP/SL koy."""
    side = "BUY" if direction == "BUY" else "SELL"
    ts = int(time.time())

    if not settings.trading_enabled:
        return

    try:
        # 1. Leverage 1x
        try:
            await set_leverage(symbol, leverage=1)
        except Exception as e:
            if "-4028" not in str(e):
                raise

        # 2. Mevcut pozisyon kontrol
        positions = await get_position_risk(symbol)
        pos_amt = 0.0
        for p in positions:
            if p.get("symbol") == symbol:
                pos_amt = float(p.get("positionAmt", 0))
                break

        # Ayni yon → pas gec
        if (side == "BUY" and pos_amt > 0) or (side == "SELL" and pos_amt < 0):
            log.info("divergence_same_direction_skip", symbol=symbol, side=side, pos_amt=pos_amt)
            return

        # Ters yon → mevcut kapat
        if pos_amt != 0:
            log.info("divergence_closing_opposite", symbol=symbol, side=side, pos_amt=pos_amt)
            await cancel_all_open_orders(symbol)
            close_side = "SELL" if pos_amt > 0 else "BUY"
            await place_market_order(symbol, close_side, abs(pos_amt), reduce_only=True)
            await asyncio.sleep(0.5)

        # 3. Bekleyen limit order varsa iptal
        if symbol in _pending_orders:
            old = _pending_orders.pop(symbol)
            try:
                from app.modules.binance_client import cancel_order
                await cancel_order(symbol, int(old["order_id"]))
                log.info("divergence_old_limit_cancelled", symbol=symbol, order_id=old["order_id"])
            except Exception:
                pass

        # 4. Exchange info + miktar hesapla
        info = await get_exchange_info(symbol)
        step_size = info["lotSize"]["stepSize"]
        min_qty = info["lotSize"]["minQty"]
        tick_size = info["priceFilter"]["tickSize"]

        balance = await get_usdt_balance()
        if balance <= 0:
            await log_trade(event_id=event_id, ts=ts, symbol=symbol, side=side,
                            status="FAILED", reason="Insufficient balance")
            return

        sym_cfg = get_symbol_config(symbol)
        weight = sym_cfg.get("weight", 0.10)
        usable = balance * weight * 0.98
        raw_qty = usable / entry_price
        quantity = round_step_size(raw_qty, step_size)

        if quantity < min_qty:
            await log_trade(event_id=event_id, ts=ts, symbol=symbol, side=side,
                            status="FAILED", reason=f"Qty {quantity} < min {min_qty}")
            return

        # 5. Limit order fiyatini yuvarla
        limit_price = round_price(entry_price, tick_size)

        # 6. Limit order koy (GTC)
        result = await place_limit_order(symbol, side, quantity, limit_price)
        order_id = str(result.get("orderId", ""))

        log.info(
            "divergence_limit_placed",
            symbol=symbol,
            side=side,
            price=limit_price,
            qty=quantity,
            order_id=order_id,
            event_id=event_id,
        )

        # 7. Pending order kaydet
        _pending_orders[symbol] = {
            "order_id": order_id,
            "side": side,
            "entry_price": limit_price,
            "quantity": quantity,
            "event_id": event_id,
            "ts": ts,
            "tp_pct": cfg.get("tp_pct", 0.010),
            "sl_pct": cfg.get("sl_pct", 0.003),
        }

        # 8. WebSocket fill callback kaydet
        register_fill_callback(symbol, _on_fill)

        await log_trade(
            event_id=event_id, ts=ts, symbol=symbol, side=side,
            quantity=quantity, entry_price=limit_price,
            order_id=order_id, status="LIMIT_PLACED",
            reason=f"Divergence {direction} limit @ {limit_price}",
        )

    except Exception as e:
        log.error("divergence_executor_error", symbol=symbol, error=str(e))
        await log_trade(event_id=event_id, ts=ts, symbol=symbol, side=side,
                        status="ERROR", reason=str(e))


async def _on_fill(event: dict) -> None:
    """WebSocket ORDER_TRADE_UPDATE → FILLED callback.

    Fill olunca hemen TP + SL koy.
    """
    symbol = event["symbol"]
    order_type = event.get("order_type", "")

    # Sadece LIMIT order fill'lerini isle (TP/SL fill'lerini degil)
    if order_type != "LIMIT":
        return

    pending = _pending_orders.pop(symbol, None)
    if pending is None:
        return

    fill_price = event.get("avg_price", 0)
    if fill_price <= 0:
        fill_price = pending["entry_price"]

    side = pending["side"]
    quantity = pending["quantity"]
    tp_pct = pending["tp_pct"]
    sl_pct = pending["sl_pct"]
    event_id = pending["event_id"]

    log.info(
        "divergence_limit_filled",
        symbol=symbol,
        side=side,
        fill_price=fill_price,
        order_id=pending["order_id"],
    )

    # TP + SL hesapla
    info = await get_exchange_info(symbol)
    tick_size = info["priceFilter"]["tickSize"]
    exit_side = "SELL" if side == "BUY" else "BUY"

    if side == "BUY":
        tp_price = round_price(fill_price * (1 + tp_pct), tick_size)
        sl_price = round_price(fill_price * (1 - sl_pct), tick_size)
    else:
        tp_price = round_price(fill_price * (1 - tp_pct), tick_size)
        sl_price = round_price(fill_price * (1 + sl_pct), tick_size)

    # TP + SL paralel koy — gecikme minimumda
    tp_ok = False
    sl_ok = False

    async def _place_tp():
        nonlocal tp_ok
        try:
            await place_take_profit_market_order(symbol, exit_side, quantity, tp_price)
            tp_ok = True
        except Exception as e:
            log.error("divergence_tp_failed", symbol=symbol, error=str(e))

    async def _place_sl():
        nonlocal sl_ok
        try:
            await place_stop_market_order(symbol, exit_side, quantity, sl_price)
            sl_ok = True
        except Exception as e:
            log.error("divergence_sl_failed", symbol=symbol, error=str(e))

    await asyncio.gather(_place_tp(), _place_sl())

    log.info(
        "divergence_tp_sl_placed",
        symbol=symbol,
        tp_price=tp_price,
        sl_price=sl_price,
        tp_ok=tp_ok,
        sl_ok=sl_ok,
    )

    await log_trade(
        event_id=event_id,
        ts=int(time.time()),
        symbol=symbol,
        side=side,
        quantity=quantity,
        entry_price=fill_price,
        stop_price=sl_price,
        status="FILLED",
        reason=f"Divergence filled @ {fill_price}, TP={tp_price} SL={sl_price}",
    )
