"""Binance USDT-M Futures API client with HMAC SHA256 signing."""

from __future__ import annotations

import hashlib
import hmac
import math
import time
from urllib.parse import urlencode

import httpx

from app.config import settings
from app.utils.logging import get_logger

log = get_logger(__name__)

_client: httpx.AsyncClient | None = None

_BASE_URL = "https://fapi.binance.com"
_TESTNET_URL = "https://testnet.binancefuture.com"


def _base_url() -> str:
    return _TESTNET_URL if settings.binance_testnet else _BASE_URL


async def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            base_url=_base_url(),
            timeout=httpx.Timeout(10.0, connect=5.0),
            headers={"X-MBX-APIKEY": settings.binance_api_key},
        )
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
        await log.ainfo("binance_client_closed")


def _sign(params: dict) -> dict:
    """Add timestamp and HMAC SHA256 signature to params."""
    params["timestamp"] = int(time.time() * 1000)
    query = urlencode(params)
    sig = hmac.new(
        settings.binance_api_secret.encode(),
        query.encode(),
        hashlib.sha256,
    ).hexdigest()
    params["signature"] = sig
    return params


async def get_exchange_info(symbol: str) -> dict:
    """Get symbol info including lot size and price precision."""
    client = await get_client()
    resp = await client.get("/fapi/v1/exchangeInfo", params={"symbol": symbol})
    resp.raise_for_status()
    data = resp.json()
    for s in data.get("symbols", []):
        if s["symbol"] == symbol:
            lot_size = {}
            price_filter = {}
            for f in s.get("filters", []):
                if f["filterType"] == "LOT_SIZE":
                    lot_size = f
                elif f["filterType"] == "PRICE_FILTER":
                    price_filter = f
            return {
                "symbol": symbol,
                "pricePrecision": s.get("pricePrecision", 2),
                "quantityPrecision": s.get("quantityPrecision", 3),
                "lotSize": {
                    "minQty": float(lot_size.get("minQty", "0.001")),
                    "maxQty": float(lot_size.get("maxQty", "1000000")),
                    "stepSize": float(lot_size.get("stepSize", "0.001")),
                },
                "priceFilter": {
                    "tickSize": float(price_filter.get("tickSize", "0.01")),
                },
            }
    raise ValueError(f"Symbol {symbol} not found in exchange info")


async def set_leverage(symbol: str, leverage: int = 1) -> dict:
    """Set leverage for a symbol."""
    client = await get_client()
    params = _sign({"symbol": symbol, "leverage": leverage})
    resp = await client.post("/fapi/v1/leverage", params=params)
    resp.raise_for_status()
    return resp.json()


async def get_position_risk(symbol: str) -> list[dict]:
    """Get current position info for a symbol."""
    client = await get_client()
    params = _sign({"symbol": symbol})
    resp = await client.get("/fapi/v2/positionRisk", params=params)
    resp.raise_for_status()
    return resp.json()


async def get_usdt_balance() -> float:
    """Get available USDT balance."""
    client = await get_client()
    params = _sign({})
    resp = await client.get("/fapi/v2/balance", params=params)
    resp.raise_for_status()
    for asset in resp.json():
        if asset.get("asset") == "USDT":
            return float(asset.get("availableBalance", 0))
    return 0.0


async def cancel_all_open_orders(symbol: str) -> dict:
    """Cancel all open orders for a symbol."""
    client = await get_client()
    params = _sign({"symbol": symbol})
    resp = await client.delete("/fapi/v1/allOpenOrders", params=params)
    resp.raise_for_status()
    return resp.json()


async def place_market_order(
    symbol: str,
    side: str,
    quantity: float,
    reduce_only: bool = False,
) -> dict:
    """Place a market order."""
    client = await get_client()
    params: dict = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": quantity,
    }
    if reduce_only:
        params["reduceOnly"] = "true"
    params = _sign(params)
    resp = await client.post("/fapi/v1/order", params=params)
    resp.raise_for_status()
    return resp.json()


async def place_stop_market_order(
    symbol: str,
    side: str,
    quantity: float,
    stop_price: float,
) -> dict:
    """Place a stop-market order (for stop-loss)."""
    client = await get_client()
    params = _sign({
        "symbol": symbol,
        "side": side,
        "type": "STOP_MARKET",
        "quantity": quantity,
        "stopPrice": stop_price,
        "closePosition": "false",
        "reduceOnly": "true",
    })
    resp = await client.post("/fapi/v1/order", params=params)
    resp.raise_for_status()
    return resp.json()


def round_step_size(quantity: float, step_size: float) -> float:
    """Round quantity down to the nearest valid step size."""
    if step_size <= 0:
        return quantity
    precision = max(0, int(round(-math.log10(step_size))))
    return round(math.floor(quantity / step_size) * step_size, precision)


def round_price(price: float, tick_size: float) -> float:
    """Round price to the nearest valid tick size."""
    if tick_size <= 0:
        return price
    precision = max(0, int(round(-math.log10(tick_size))))
    return round(round(price / tick_size) * tick_size, precision)
