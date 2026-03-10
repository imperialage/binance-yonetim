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


class BinanceAPIError(Exception):
    """Binance API error with code and message from response body."""

    def __init__(self, status_code: int, code: int, msg: str, url: str):
        self.status_code = status_code
        self.code = code
        self.msg = msg
        self.url = url
        super().__init__(f"Binance {status_code}: code={code} msg='{msg}' url={url}")


def _raise_for_binance(resp: httpx.Response) -> None:
    """Raise BinanceAPIError with full detail if response is not 2xx."""
    if resp.is_success:
        return
    try:
        body = resp.json()
        code = body.get("code", resp.status_code)
        msg = body.get("msg", resp.text[:200])
    except Exception:
        code = resp.status_code
        msg = resp.text[:200]
    raise BinanceAPIError(resp.status_code, code, msg, str(resp.url))


def _base_url() -> str:
    return _TESTNET_URL if settings.binance_testnet else _BASE_URL


async def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        proxy_url = settings.binance_proxy_url or None
        _client = httpx.AsyncClient(
            base_url=_base_url(),
            timeout=httpx.Timeout(10.0, connect=5.0),
            headers={"X-MBX-APIKEY": settings.binance_api_key},
            proxy=proxy_url,
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
    _raise_for_binance(resp)
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
    _raise_for_binance(resp)
    return resp.json()


async def get_position_risk(symbol: str) -> list[dict]:
    """Get current position info for a symbol."""
    client = await get_client()
    params = _sign({"symbol": symbol})
    resp = await client.get("/fapi/v2/positionRisk", params=params)
    _raise_for_binance(resp)
    return resp.json()


async def get_usdt_balance() -> float:
    """Get available USDT balance."""
    client = await get_client()
    params = _sign({})
    resp = await client.get("/fapi/v2/balance", params=params)
    _raise_for_binance(resp)
    for asset in resp.json():
        if asset.get("asset") == "USDT":
            return float(asset.get("availableBalance", 0))
    return 0.0


async def cancel_all_open_orders(symbol: str) -> dict:
    """Cancel all open orders (regular + algo) for a symbol."""
    client = await get_client()
    # Cancel regular orders
    params = _sign({"symbol": symbol})
    resp = await client.delete("/fapi/v1/allOpenOrders", params=params)
    _raise_for_binance(resp)
    result = resp.json()
    # Cancel algo (conditional) orders
    try:
        algo_params = _sign({"symbol": symbol})
        algo_resp = await client.get("/fapi/v1/openAlgoOrders", params=algo_params)
        algo_resp.raise_for_status()
        for order in algo_resp.json():
            algo_id = order.get("algoId")
            if algo_id and order.get("symbol") == symbol:
                del_params = _sign({"algoId": algo_id})
                await client.delete("/fapi/v1/algoOrder", params=del_params)
    except Exception:
        pass  # Best-effort algo cancellation
    return result


async def place_limit_order(
    symbol: str,
    side: str,
    quantity: float,
    price: float,
) -> dict:
    """Place a limit order with GTC time-in-force."""
    client = await get_client()
    params = _sign({
        "symbol": symbol,
        "side": side,
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": quantity,
        "price": price,
    })
    resp = await client.post("/fapi/v1/order", params=params)
    _raise_for_binance(resp)
    return resp.json()


async def get_order_status(symbol: str, order_id: int) -> dict:
    """Get order status by orderId."""
    client = await get_client()
    params = _sign({"symbol": symbol, "orderId": order_id})
    resp = await client.get("/fapi/v1/order", params=params)
    _raise_for_binance(resp)
    return resp.json()


async def cancel_order(symbol: str, order_id: int) -> dict:
    """Cancel an open order by orderId."""
    client = await get_client()
    params = _sign({"symbol": symbol, "orderId": order_id})
    resp = await client.delete("/fapi/v1/order", params=params)
    _raise_for_binance(resp)
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
    _raise_for_binance(resp)
    return resp.json()


async def place_stop_market_order(
    symbol: str,
    side: str,
    quantity: float,
    stop_price: float,
) -> dict:
    """Place a stop-market order (for stop-loss) via Algo Order API."""
    client = await get_client()
    params = _sign({
        "symbol": symbol,
        "side": side,
        "type": "STOP_MARKET",
        "algoType": "CONDITIONAL",
        "quantity": quantity,
        "triggerPrice": stop_price,
        "reduceOnly": "true",
    })
    resp = await client.post("/fapi/v1/algoOrder", params=params)
    _raise_for_binance(resp)
    return resp.json()


async def place_take_profit_market_order(
    symbol: str,
    side: str,
    quantity: float,
    trigger_price: float,
) -> dict:
    """Place a take-profit-market order via Algo Order API."""
    client = await get_client()
    params = _sign({
        "symbol": symbol,
        "side": side,
        "type": "TAKE_PROFIT_MARKET",
        "algoType": "CONDITIONAL",
        "quantity": quantity,
        "triggerPrice": trigger_price,
        "reduceOnly": "true",
    })
    resp = await client.post("/fapi/v1/algoOrder", params=params)
    _raise_for_binance(resp)
    return resp.json()


async def get_income_history(
    symbol: str = "ETHUSDT",
    income_type: str = "REALIZED_PNL",
    days: int = 7,
) -> list[dict]:
    """Get realized PnL history from Binance (last N days), with pagination."""
    client = await get_client()
    all_records: list[dict] = []
    cursor_time = int((time.time() - days * 86400) * 1000)
    for _ in range(10):  # max 10 pages = 10000 records
        params = _sign({
            "symbol": symbol,
            "incomeType": income_type,
            "startTime": cursor_time,
            "limit": 1000,
        })
        resp = await client.get("/fapi/v1/income", params=params)
        _raise_for_binance(resp)
        batch = resp.json()
        if not batch:
            break
        all_records.extend(batch)
        if len(batch) < 1000:
            break
        # Move cursor past the last record
        cursor_time = int(batch[-1].get("time", 0)) + 1
    return all_records


async def get_user_trades(
    symbol: str = "ETHUSDT",
    days: int = 7,
) -> list[dict]:
    """Get detailed trade history with prices from Binance (last N days), with pagination."""
    client = await get_client()
    all_trades: list[dict] = []
    start_time = int((time.time() - days * 86400) * 1000)
    from_id: int | None = None
    for _ in range(10):  # max 10 pages = 10000 records
        p: dict = {"symbol": symbol, "limit": 1000}
        if from_id is not None:
            p["fromId"] = from_id
        else:
            p["startTime"] = start_time
        params = _sign(p)
        resp = await client.get("/fapi/v1/userTrades", params=params)
        _raise_for_binance(resp)
        batch = resp.json()
        if not batch:
            break
        all_trades.extend(batch)
        if len(batch) < 1000:
            break
        # Next page starts after the last trade id
        from_id = int(batch[-1].get("id", 0)) + 1
    return all_trades


async def get_all_orders(
    symbol: str = "ETHUSDT",
    days: int = 3,
) -> list[dict]:
    """Get all orders (filled, cancelled, etc.) from Binance Futures."""
    client = await get_client()
    start_time = int((time.time() - days * 86400) * 1000)
    params = _sign({"symbol": symbol, "startTime": start_time, "limit": 100})
    resp = await client.get("/fapi/v1/allOrders", params=params)
    _raise_for_binance(resp)
    return resp.json()


async def get_algo_orders(
    symbol: str = "ETHUSDT",
) -> list[dict]:
    """Get algo/conditional order history from Binance Futures."""
    client = await get_client()
    params = _sign({"symbol": symbol})
    resp = await client.get("/fapi/v1/algoOrder/openOrders", params=params)
    _raise_for_binance(resp)
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
