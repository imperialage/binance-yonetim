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
_client_b: httpx.AsyncClient | None = None  # 2. Binance hesabi

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
    global _client, _client_b
    if _client is not None:
        await _client.aclose()
        _client = None
    if _client_b is not None:
        await _client_b.aclose()
        _client_b = None
    await log.ainfo("binance_clients_closed")


def _sign(params: dict) -> dict:
    """Add timestamp and HMAC SHA256 signature to params (Hesap A)."""
    params["timestamp"] = int(time.time() * 1000)
    query = urlencode(params)
    sig = hmac.new(
        settings.binance_api_secret.encode(),
        query.encode(),
        hashlib.sha256,
    ).hexdigest()
    params["signature"] = sig
    return params


def _sign_b(params: dict) -> dict:
    """Add timestamp and HMAC SHA256 signature to params (Hesap B)."""
    params["timestamp"] = int(time.time() * 1000)
    query = urlencode(params)
    sig = hmac.new(
        settings.binance_api_secret_b.encode(),
        query.encode(),
        hashlib.sha256,
    ).hexdigest()
    params["signature"] = sig
    return params


# ── 2. Binance Hesabi (B) ──────────────────────────────


async def get_client_b() -> httpx.AsyncClient:
    """2. Binance hesabi client'i."""
    global _client_b
    if _client_b is None:
        if not settings.binance_api_key_b:
            raise BinanceAPIError(0, -1, "2. Binance hesabi API key ayarlanmamis (BINANCE_API_KEY_B)", "")
        proxy_url = settings.binance_proxy_url or None
        _client_b = httpx.AsyncClient(
            base_url=_base_url(),
            timeout=httpx.Timeout(10.0, connect=5.0),
            headers={"X-MBX-APIKEY": settings.binance_api_key_b},
            proxy=proxy_url,
        )
    return _client_b


async def get_position_risk_b(symbol: str) -> list[dict]:
    """Get position info — Hesap B."""
    client = await get_client_b()
    params = _sign_b({"symbol": symbol})
    resp = await client.get("/fapi/v2/positionRisk", params=params)
    _raise_for_binance(resp)
    return resp.json()


async def get_total_wallet_balance_b() -> float:
    """Get total wallet balance — Hesap B."""
    client = await get_client_b()
    params = _sign_b({})
    resp = await client.get("/fapi/v2/balance", params=params)
    _raise_for_binance(resp)
    for asset in resp.json():
        if asset.get("asset") == "USDT":
            return float(asset.get("balance", 0))
    return 0.0


async def place_market_order_b(symbol: str, side: str, quantity: float, reduce_only: bool = False) -> dict:
    """Place market order — Hesap B."""
    client = await get_client_b()
    params: dict = {"symbol": symbol, "side": side, "type": "MARKET", "quantity": quantity}
    if reduce_only:
        params["reduceOnly"] = "true"
    params = _sign_b(params)
    resp = await client.post("/fapi/v1/order", params=params)
    _raise_for_binance(resp)
    return resp.json()


async def cancel_all_open_orders_b(symbol: str) -> dict:
    """Cancel all open orders — Hesap B (regular + algo)."""
    client = await get_client_b()
    cancelled_regular = 0
    cancelled_algo = 0

    # 1. Regular orders toplu sil
    try:
        params = _sign_b({"symbol": symbol})
        resp = await client.delete("/fapi/v1/allOpenOrders", params=params)
        _raise_for_binance(resp)
        cancelled_regular = 1
    except Exception as e:
        log.warning("bulk_cancel_b_failed", symbol=symbol, error=str(e))

    # 2. Tracked algo emirlerini sil (B hesap)
    async with _algo_lock_b:
        data = _load_algo_ids_b()
        algo_ids = data.pop(symbol, [])
        _save_algo_ids_b(data)

    for aid in algo_ids:
        try:
            del_params = _sign_b({"algoId": aid})
            del_resp = await client.delete("/fapi/v1/algoOrder", params=del_params)
            del_resp.raise_for_status()
            cancelled_algo += 1
            log.info("algo_order_b_cancelled", symbol=symbol, algo_id=aid)
        except Exception as e:
            log.info("algo_cancel_b_skip", symbol=symbol, algo_id=aid, error=str(e))

    log.info("all_orders_b_cancelled", symbol=symbol, regular=cancelled_regular, algo=cancelled_algo)
    return {"regular": cancelled_regular, "algo": cancelled_algo}


async def set_leverage_b(symbol: str, leverage: int = 1) -> dict:
    """Set leverage — Hesap B."""
    client = await get_client_b()
    params = _sign_b({"symbol": symbol, "leverage": leverage})
    resp = await client.post("/fapi/v1/leverage", params=params)
    _raise_for_binance(resp)
    return resp.json()


def is_account_b_configured() -> bool:
    """2. Binance hesabi yapilandirilmis mi?"""
    return bool(settings.binance_api_key_b and settings.binance_api_secret_b)


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


async def get_total_wallet_balance() -> float:
    """Get TOTAL USDT wallet balance (dahil acik pozisyonlarin margin'i).
    Weight hesabi icin kullanilir — her sembol toplam bakiye yuzdesi alir.
    """
    client = await get_client()
    params = _sign({})
    resp = await client.get("/fapi/v2/balance", params=params)
    _raise_for_binance(resp)
    for asset in resp.json():
        if asset.get("asset") == "USDT":
            return float(asset.get("balance", 0))
    return 0.0


# ── Algo order ID tracking — persistent JSON file ──
# algoOrder/openOrders endpoint 404 verdigi icin, koyduklarimizi takip ediyoruz
# Dosyada sakliyoruz ki deploy/restart sonrasi kaybolmasin.
import json as _json
import os as _os
from pathlib import Path as _Path

_ALGO_IDS_PATH = _Path(_os.getenv("DATA_DIR", "data")) / "algo_ids.json"
_ALGO_IDS_B_PATH = _Path(_os.getenv("DATA_DIR", "data")) / "algo_ids_b.json"

import asyncio as _asyncio
_algo_lock = _asyncio.Lock()
_algo_lock_b = _asyncio.Lock()


def _load_algo_ids() -> dict[str, list[int]]:
    """Load tracked algo IDs from disk."""
    try:
        if _ALGO_IDS_PATH.exists():
            return _json.loads(_ALGO_IDS_PATH.read_text())
    except Exception:
        pass
    return {}


def _save_algo_ids(data: dict[str, list[int]]) -> None:
    """Save tracked algo IDs to disk."""
    try:
        _ALGO_IDS_PATH.parent.mkdir(parents=True, exist_ok=True)
        _ALGO_IDS_PATH.write_text(_json.dumps(data))
    except Exception as e:
        log.warning("algo_ids_save_failed", error=str(e))


def _load_algo_ids_b() -> dict[str, list[int]]:
    """Load tracked algo IDs from disk — Hesap B."""
    try:
        if _ALGO_IDS_B_PATH.exists():
            return _json.loads(_ALGO_IDS_B_PATH.read_text())
    except Exception:
        pass
    return {}


def _save_algo_ids_b(data: dict[str, list[int]]) -> None:
    """Save tracked algo IDs to disk — Hesap B."""
    try:
        _ALGO_IDS_B_PATH.parent.mkdir(parents=True, exist_ok=True)
        _ALGO_IDS_B_PATH.write_text(_json.dumps(data))
    except Exception as e:
        log.warning("algo_ids_b_save_failed", error=str(e))


async def _track_algo_id_b_async(symbol: str, algo_id: int | str | None) -> None:
    """Track an algo order ID for Hesap B (persistent, thread-safe)."""
    if algo_id is None:
        return
    aid = int(algo_id)
    async with _algo_lock_b:
        data = _load_algo_ids_b()
        if symbol not in data:
            data[symbol] = []
        if aid not in data[symbol]:
            data[symbol].append(aid)
        _save_algo_ids_b(data)


async def _track_algo_id_async(symbol: str, algo_id: int | str | None) -> None:
    """Track an algo order ID for later cancellation (persistent, thread-safe)."""
    if algo_id is None:
        return
    aid = int(algo_id)
    async with _algo_lock:
        data = _load_algo_ids()
        if symbol not in data:
            data[symbol] = []
        if aid not in data[symbol]:
            data[symbol].append(aid)
        _save_algo_ids(data)


def _track_algo_id(symbol: str, algo_id: int | str | None) -> None:
    """Sync wrapper — eski cagirilari kirma. Yeni kodda _track_algo_id_async kullan."""
    if algo_id is None:
        return
    aid = int(algo_id)
    data = _load_algo_ids()
    if symbol not in data:
        data[symbol] = []
    if aid not in data[symbol]:
        data[symbol].append(aid)
    _save_algo_ids(data)


async def cancel_all_open_orders(symbol: str) -> dict:
    """Cancel all open orders for a symbol.

    1. DELETE /fapi/v1/allOpenOrders — regular emirler
    2. Tracked algo ID'leri tek tek DELETE /fapi/v1/algoOrder ile sil
    3. Track listesini temizle ve dosyaya kaydet
    """
    client = await get_client()
    cancelled_regular = 0
    cancelled_algo = 0

    # ── 1. Regular orders toplu sil ──
    try:
        params = _sign({"symbol": symbol})
        resp = await client.delete("/fapi/v1/allOpenOrders", params=params)
        _raise_for_binance(resp)
        cancelled_regular = 1
    except Exception as e:
        log.warning("bulk_cancel_failed", symbol=symbol, error=str(e))

    # ── 2. Tracked algo emirlerini sil (persistent file, lock altinda) ──
    async with _algo_lock:
        data = _load_algo_ids()
        algo_ids = data.pop(symbol, [])
        _save_algo_ids(data)  # once dosyadan temizle

    for aid in algo_ids:
        try:
            del_params = _sign({"algoId": aid})
            del_resp = await client.delete("/fapi/v1/algoOrder", params=del_params)
            del_resp.raise_for_status()
            cancelled_algo += 1
            log.info("algo_order_cancelled", symbol=symbol, algo_id=aid)
        except Exception as e:
            log.info("algo_cancel_skip", symbol=symbol, algo_id=aid, error=str(e))

    total = cancelled_regular + cancelled_algo
    log.info("all_orders_cancelled", symbol=symbol, regular=cancelled_regular, algo=cancelled_algo, total=total)
    return {"regular": cancelled_regular, "algo": cancelled_algo, "total": total}


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


async def place_stop_market_instant(
    symbol: str,
    side: str,
    quantity: float,
    stop_price: float,
) -> dict:
    """Place STOP_MARKET order — giris emriyle ayni anda konur.

    reduceOnly YOK — pozisyon olmadan da konabilir.
    algoOrder API kullanir (normal order API STOP_MARKET desteklemiyor).
    """
    client = await get_client()
    params = _sign({
        "symbol": symbol,
        "side": side,
        "type": "STOP_MARKET",
        "algoType": "CONDITIONAL",
        "quantity": quantity,
        "triggerPrice": stop_price,
    })
    resp = await client.post("/fapi/v1/algoOrder", params=params)
    _raise_for_binance(resp)
    result = resp.json()
    await _track_algo_id_async(symbol, result.get("algoId"))
    log.info("sl_instant_placed", symbol=symbol, side=side, stop_price=stop_price, algo_id=result.get("algoId"))
    return result


async def place_stop_market_instant_b(
    symbol: str,
    side: str,
    quantity: float,
    stop_price: float,
) -> dict:
    """Place STOP_MARKET order — Hesap B. Algo tracking dahil."""
    client = await get_client_b()
    params = _sign_b({
        "symbol": symbol,
        "side": side,
        "type": "STOP_MARKET",
        "algoType": "CONDITIONAL",
        "quantity": quantity,
        "triggerPrice": stop_price,
    })
    resp = await client.post("/fapi/v1/algoOrder", params=params)
    _raise_for_binance(resp)
    result = resp.json()
    await _track_algo_id_b_async(symbol, result.get("algoId"))
    log.info("sl_instant_b_placed", symbol=symbol, side=side, stop_price=stop_price, algo_id=result.get("algoId"))
    return result


async def place_stop_market_order(
    symbol: str,
    side: str,
    quantity: float,
    stop_price: float,
) -> dict:
    """Place a STOP_MARKET order (for stop-loss) via Algo Order API."""
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
    result = resp.json()
    await _track_algo_id_async(symbol, result.get("algoId"))
    log.info("sl_order_placed", symbol=symbol, side=side, trigger=stop_price, algo_id=result.get("algoId"))
    return result


async def place_take_profit_market_order(
    symbol: str,
    side: str,
    quantity: float,
    trigger_price: float,
) -> dict:
    """Place a TAKE_PROFIT_MARKET order via Algo Order API."""
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
    result = resp.json()
    await _track_algo_id_async(symbol, result.get("algoId"))
    log.info("tp_order_placed", symbol=symbol, side=side, trigger=trigger_price, algo_id=result.get("algoId"))
    return result


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
    """Get all orders (filled, cancelled, etc.) from Binance Futures, with pagination."""
    client = await get_client()
    all_orders: list[dict] = []
    start_time = int((time.time() - days * 86400) * 1000)
    for _ in range(5):  # max 5 pages = 2500 orders
        p: dict = {"symbol": symbol, "startTime": start_time, "limit": 500}
        params = _sign(p)
        resp = await client.get("/fapi/v1/allOrders", params=params)
        _raise_for_binance(resp)
        batch = resp.json()
        if not batch:
            break
        all_orders.extend(batch)
        if len(batch) < 500:
            break
        start_time = int(batch[-1].get("time", 0)) + 1
    return all_orders


async def get_algo_orders_history(
    symbol: str = "ETHUSDT",
) -> list[dict]:
    """Get algo/conditional order history (open + historical) from Binance Futures."""
    client = await get_client()
    all_orders: list[dict] = []

    # Open algo orders
    # NOT: Binance 2025-12'de algo endpoint'ini migrate etti; eski path
    # `/fapi/v1/algoOrder/openOrders` artik 404 donuyor. Dogru path:
    # `/fapi/v1/openAlgoOrders`. Response schema da list-of-orders direkt
    # (data.orders sarmayan).
    try:
        params = _sign({"symbol": symbol})
        resp = await client.get("/fapi/v1/openAlgoOrders", params=params)
        _raise_for_binance(resp)
        data = resp.json()
        if isinstance(data, list):
            all_orders.extend(data)
        elif isinstance(data, dict):
            all_orders.extend(data.get("orders", []))
    except Exception:
        pass

    # Historical algo orders
    try:
        params = _sign({"symbol": symbol, "pageSize": 100})
        resp = await client.get("/fapi/v1/algoOrder/historicalOrders", params=params)
        _raise_for_binance(resp)
        data = resp.json()
        all_orders.extend(data.get("orders", []) if isinstance(data, dict) else [])
    except Exception:
        pass

    return all_orders


async def get_funding_rate(symbol: str) -> float | None:
    """Get the latest funding rate for a symbol (unsigned endpoint)."""
    client = await get_client()
    resp = await client.get(
        "/fapi/v1/premiumIndex", params={"symbol": symbol}
    )
    _raise_for_binance(resp)
    data = resp.json()
    rate_str = data.get("lastFundingRate")
    if rate_str is not None:
        return float(rate_str)
    return None


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
