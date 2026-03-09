"""Admin endpoints – runtime config, event management & manual trading."""

from __future__ import annotations

import json
import math

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel

from app.config import settings
from app.modules.binance_client import (
    BinanceAPIError,
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
from app.modules.redis_client import get_redis
from app.schemas.config import RuntimeConfig
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()


# ── Request models ──────────────────────────────────────
class ManualCloseRequest(BaseModel):
    symbol: str = "ETHUSDT"


class ManualOpenRequest(BaseModel):
    symbol: str = "ETHUSDT"
    side: str  # "BUY" or "SELL"
    entry_price: float | None = None  # None → MARKET, set → LIMIT
    sl_price: float | None = None     # None → strategy %, set → custom
    tp_price: float | None = None     # None → strategy %, set → custom


@router.post("/config", response_model=RuntimeConfig)
async def update_config(
    body: RuntimeConfig,
    x_admin_token: str = Header(...),
) -> RuntimeConfig:
    if x_admin_token != settings.admin_token:
        raise HTTPException(status_code=401, detail="Invalid admin token")

    r = await get_redis()
    await r.set("tv:config", body.model_dump_json())

    log.info("config_updated", config=body.model_dump())
    return body


@router.delete("/events/{symbol}")
async def delete_event(
    symbol: str,
    event_id: str = Query(...),
    x_admin_token: str = Header(...),
) -> dict:
    if x_admin_token != settings.admin_token:
        raise HTTPException(status_code=401, detail="Invalid admin token")

    r = await get_redis()
    key = f"tv:events:{symbol.upper()}"
    raw_list = await r.lrange(key, 0, -1)

    removed = 0
    for raw in raw_list:
        try:
            ev = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        if ev.get("event_id") == event_id:
            await r.lrem(key, 1, raw)
            removed += 1

    log.info("event_deleted", symbol=symbol, event_id=event_id, removed=removed)
    return {"deleted": removed, "event_id": event_id}


# ── Manual Trading ──────────────────────────────────────


@router.post("/manual/close")
async def manual_close_position(
    body: ManualCloseRequest,
    x_admin_token: str = Header(...),
) -> dict:
    """Close the open position for a symbol with a market order."""
    if x_admin_token != settings.admin_token:
        raise HTTPException(status_code=401, detail="Invalid admin token")

    symbol = body.symbol.upper()

    # Check current position
    positions = await get_position_risk(symbol)
    pos_amt = 0.0
    for p in positions:
        if p.get("symbol") == symbol:
            pos_amt = float(p.get("positionAmt", 0))
            break

    if pos_amt == 0:
        raise HTTPException(status_code=400, detail="No open position")

    # Cancel all open SL/TP orders
    await cancel_all_open_orders(symbol)

    # Close position with market order
    close_side = "SELL" if pos_amt > 0 else "BUY"
    close_qty = abs(pos_amt)
    result = await place_market_order(symbol, close_side, close_qty, reduce_only=True)

    entry_price = float(result.get("avgPrice", 0))
    log.info("manual_close", symbol=symbol, side=close_side, qty=close_qty, price=entry_price)

    return {
        "ok": True,
        "side": close_side,
        "qty": close_qty,
        "price": entry_price,
    }


@router.post("/manual/open")
async def manual_open_position(
    body: ManualOpenRequest,
    x_admin_token: str = Header(...),
) -> dict:
    """Open a new position with SL/TP using the active strategy."""
    if x_admin_token != settings.admin_token:
        raise HTTPException(status_code=401, detail="Invalid admin token")

    symbol = body.symbol.upper()
    side = body.side.upper()
    if side not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="side must be BUY or SELL")

    try:
        # Check current position
        positions = await get_position_risk(symbol)
        pos_amt = 0.0
        for p in positions:
            if p.get("symbol") == symbol:
                pos_amt = float(p.get("positionAmt", 0))
                break

        # Same direction already open → reject
        if (side == "BUY" and pos_amt > 0) or (side == "SELL" and pos_amt < 0):
            raise HTTPException(status_code=400, detail="Same direction position already open")

        # Opposite position open → close it first
        if pos_amt != 0:
            await cancel_all_open_orders(symbol)
            close_side = "SELL" if pos_amt > 0 else "BUY"
            await place_market_order(symbol, close_side, abs(pos_amt), reduce_only=True)
            log.info("manual_open_closed_opposite", symbol=symbol, closed_side=close_side, qty=abs(pos_amt))

        # Set leverage
        try:
            await set_leverage(symbol, leverage=1)
        except BinanceAPIError as e:
            if "-4028" not in str(e):
                raise

        # Get exchange info for rounding
        info = await get_exchange_info(symbol)
        step_size = info["lotSize"]["stepSize"]
        min_qty = info["lotSize"]["minQty"]
        tick_size = info["priceFilter"]["tickSize"]

        # Calculate position size — sabit ~21 USDT (min notional 20 USDT + rounding buffer)
        target_usdt = 21.0
        # Get current price from position risk or mark price
        mark_price = 0.0
        fresh_positions = await get_position_risk(symbol)
        for p in fresh_positions:
            if p.get("symbol") == symbol:
                mark_price = float(p.get("markPrice", 0))
                break
        if mark_price <= 0:
            raise HTTPException(status_code=400, detail="Cannot determine current price")

        raw_qty = target_usdt / mark_price
        # Yukarı yuvarla: min notional'ın altına düşmemesi için
        precision = max(0, int(round(-math.log10(step_size)))) if step_size > 0 else 3
        quantity = round(math.ceil(raw_qty / step_size) * step_size, precision)
        if quantity < min_qty:
            raise HTTPException(status_code=400, detail=f"Insufficient balance: qty={quantity} < min={min_qty}")

        # Place entry order: LIMIT if entry_price given, else MARKET
        if body.entry_price is not None:
            limit_price = round_price(body.entry_price, tick_size)
            order_result = await place_limit_order(symbol, side, quantity, limit_price)
            entry_price = limit_price
        else:
            order_result = await place_market_order(symbol, side, quantity)
            # Gerçek giriş fiyatını pozisyon verisinden al (avgPrice güvenilmez olabilir)
            import asyncio
            await asyncio.sleep(0.3)  # Pozisyon güncellemesi için kısa bekle
            pos_data = await get_position_risk(symbol)
            entry_price = 0.0
            current_mark = 0.0
            for p in pos_data:
                if p.get("symbol") == symbol:
                    entry_price = float(p.get("entryPrice", 0))
                    current_mark = float(p.get("markPrice", 0))
                    break
            if entry_price <= 0:
                entry_price = float(order_result.get("avgPrice", 0))
            if entry_price <= 0:
                entry_price = mark_price
            if current_mark <= 0:
                current_mark = entry_price

        # SL/TP: use custom values if provided, otherwise calculate from strategy
        active_tf = settings.trading_timeframes.split(",")[0].strip()
        sl_pct, tp_pct = settings.get_strategy(active_tf)

        if side == "BUY":
            sl_side = "SELL"
            tp_side = "SELL"
            raw_sl = body.sl_price if body.sl_price is not None else entry_price * (1 - sl_pct)
            raw_tp = body.tp_price if body.tp_price is not None else entry_price * (1 + tp_pct)
        else:
            sl_side = "BUY"
            tp_side = "BUY"
            raw_sl = body.sl_price if body.sl_price is not None else entry_price * (1 + sl_pct)
            raw_tp = body.tp_price if body.tp_price is not None else entry_price * (1 - tp_pct)

        sl_price = round_price(raw_sl, tick_size)
        tp_price = round_price(raw_tp, tick_size)

        # SL/TP'nin mevcut fiyata göre doğru tarafta olduğunu kontrol et
        ref_price = current_mark if body.entry_price is None else entry_price
        if side == "BUY":
            # SL mark'ın altında, TP mark'ın üstünde olmalı
            if sl_price >= ref_price:
                sl_price = round_price(ref_price * (1 - sl_pct), tick_size)
            if tp_price <= ref_price:
                tp_price = round_price(ref_price * (1 + tp_pct), tick_size)
        else:
            # SL mark'ın üstünde, TP mark'ın altında olmalı
            if sl_price <= ref_price:
                sl_price = round_price(ref_price * (1 + sl_pct), tick_size)
            if tp_price >= ref_price:
                tp_price = round_price(ref_price * (1 - tp_pct), tick_size)

        await place_stop_market_order(symbol, sl_side, quantity, sl_price)
        await place_take_profit_market_order(symbol, tp_side, quantity, tp_price)

        log.info(
            "manual_open",
            symbol=symbol, side=side, qty=quantity,
            entry=entry_price, sl=sl_price, tp=tp_price,
        )

        return {
            "ok": True,
            "side": side,
            "qty": quantity,
            "entry_price": entry_price,
            "sl_price": sl_price,
            "tp_price": tp_price,
        }

    except HTTPException:
        raise
    except BinanceAPIError as e:
        log.error("manual_open_binance_error", symbol=symbol, side=side, error=str(e))
        raise HTTPException(status_code=400, detail=f"Binance API: {e.msg}")
    except Exception as e:
        log.error("manual_open_error", symbol=symbol, side=side, error=str(e), error_type=type(e).__name__)
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
