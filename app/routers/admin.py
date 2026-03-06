"""Admin endpoints – runtime config, event management & manual trading."""

from __future__ import annotations

import json

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel

from app.config import settings
from app.modules.binance_client import (
    BinanceAPIError,
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

    # Calculate position size from balance
    balance = await get_usdt_balance()
    usable = balance * 0.98
    # Get current price from a small position risk or mark price
    mark_price = 0.0
    fresh_positions = await get_position_risk(symbol)
    for p in fresh_positions:
        if p.get("symbol") == symbol:
            mark_price = float(p.get("markPrice", 0))
            break
    if mark_price <= 0:
        raise HTTPException(status_code=400, detail="Cannot determine current price")

    raw_qty = usable / mark_price
    quantity = round_step_size(raw_qty, step_size)
    if quantity < min_qty:
        raise HTTPException(status_code=400, detail=f"Insufficient balance: qty={quantity} < min={min_qty}")

    # Place market entry
    order_result = await place_market_order(symbol, side, quantity)
    entry_price = float(order_result.get("avgPrice", 0))
    if entry_price <= 0:
        entry_price = mark_price

    # Get SL/TP from active strategy
    active_tf = settings.trading_timeframes.split(",")[0].strip()
    sl_pct, tp_pct = settings.get_strategy(active_tf)

    # Calculate & place SL
    if side == "BUY":
        raw_sl = entry_price * (1 - sl_pct)
        raw_tp = entry_price * (1 + tp_pct)
        sl_side = "SELL"
        tp_side = "SELL"
    else:
        raw_sl = entry_price * (1 + sl_pct)
        raw_tp = entry_price * (1 - tp_pct)
        sl_side = "BUY"
        tp_side = "BUY"

    sl_price = round_price(raw_sl, tick_size)
    tp_price = round_price(raw_tp, tick_size)

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
