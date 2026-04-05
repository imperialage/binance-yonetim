"""WebSocket USER_DATA stream — order fill callback.

Binance Futures USER_DATA stream ile limit order fill tespiti.
Fill aninda callback cagrilir → TP + SL konur.

Pattern: price_stream.py ile ayni lifecycle.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Coroutine

import httpx
import websockets

from app.config import settings
from app.utils.logging import get_logger

log = get_logger(__name__)

BINANCE_FAPI_BASE = "https://fapi.binance.com"
TESTNET_FAPI_BASE = "https://testnet.binancefuture.com"
BINANCE_WS_BASE = "wss://fstream.binance.com/ws/"
TESTNET_WS_BASE = "wss://stream.binancefuture.com/ws/"

LISTEN_KEY_RENEW_INTERVAL = 30 * 60  # 30 dakika
RECONNECT_DELAY = 3

_task: asyncio.Task[None] | None = None
_listen_key: str | None = None

# Fill callback registry: symbol → async callback(event_data)
_fill_callbacks: dict[str, Callable[..., Coroutine]] = {}


def register_fill_callback(symbol: str, callback: Callable[..., Coroutine]) -> None:
    """Bir sembol icin fill callback kaydet."""
    _fill_callbacks[symbol.upper()] = callback
    log.info("fill_callback_registered", symbol=symbol.upper())


def unregister_fill_callback(symbol: str) -> None:
    """Fill callback kaldir."""
    _fill_callbacks.pop(symbol.upper(), None)


def _base_url() -> str:
    return TESTNET_FAPI_BASE if settings.binance_testnet else BINANCE_FAPI_BASE


def _ws_base() -> str:
    return TESTNET_WS_BASE if settings.binance_testnet else BINANCE_WS_BASE


async def _get_listen_key() -> str:
    """POST /fapi/v1/listenKey → listenKey al."""
    proxy_url = settings.binance_proxy_url or None
    async with httpx.AsyncClient(timeout=10, proxy=proxy_url) as client:
        resp = await client.post(
            f"{_base_url()}/fapi/v1/listenKey",
            headers={"X-MBX-APIKEY": settings.binance_api_key},
        )
        resp.raise_for_status()
        key = resp.json().get("listenKey", "")
        if not key:
            raise ValueError("Empty listenKey from Binance")
        return key


async def _renew_listen_key(key: str) -> None:
    """PUT /fapi/v1/listenKey → listenKey yenile."""
    proxy_url = settings.binance_proxy_url or None
    async with httpx.AsyncClient(timeout=10, proxy=proxy_url) as client:
        resp = await client.put(
            f"{_base_url()}/fapi/v1/listenKey",
            headers={"X-MBX-APIKEY": settings.binance_api_key},
        )
        resp.raise_for_status()


async def _handle_event(data: dict[str, Any]) -> None:
    """USER_DATA stream event isleme."""
    event_type = data.get("e", "")

    if event_type == "ORDER_TRADE_UPDATE":
        order = data.get("o", {})
        symbol = order.get("s", "")
        status = order.get("X", "")  # Order status
        order_type = order.get("ot", "")  # Original order type
        side = order.get("S", "")
        avg_price = float(order.get("ap", 0))
        qty = float(order.get("q", 0))
        order_id = order.get("i", "")

        await log.ainfo(
            "order_update",
            symbol=symbol,
            status=status,
            type=order_type,
            side=side,
            avg_price=avg_price,
            order_id=order_id,
        )

        # FILLED event → callback cagir
        if status == "FILLED" and symbol in _fill_callbacks:
            try:
                await _fill_callbacks[symbol]({
                    "symbol": symbol,
                    "side": side,
                    "avg_price": avg_price,
                    "qty": qty,
                    "order_id": str(order_id),
                    "order_type": order_type,
                })
            except Exception as e:
                await log.aerror("fill_callback_error", symbol=symbol, error=str(e))

        # FILLED event → signal_engine'e bildir (pozisyon acildi/kapandi)
        if status == "FILLED":
            try:
                from app.modules.signal_engine import get_engine
                engine = get_engine(symbol)
                if engine:
                    reduce_only = order.get("R", False)  # reduceOnly flag
                    close_pos = order.get("cp", False)    # closePosition flag
                    if reduce_only or close_pos:
                        async with engine._state_lock:
                            if not engine.has_position:
                                return  # zaten kapali — polling halletmis
                            # Pozisyon kapandi — ONCE eski emirleri temizle, SONRA state sifirla
                            try:
                                from app.modules.binance_client import cancel_all_open_orders
                                await cancel_all_open_orders(symbol)
                                await log.ainfo("position_closed_orders_cleaned_instant", symbol=symbol)
                            except Exception:
                                pass
                            engine.on_position_closed()
                            await log.ainfo("signal_engine_position_closed", symbol=symbol, side=side)
                            # Son sinyal hala gecerli mi? Hemen isleme gir
                            if engine.last_signal and engine.last_signal_bar == engine.candle_start:
                                sig = engine.last_signal
                                should = await engine.try_execute_signal(sig)
                                if should:
                                    from app.config import settings as app_cfg
                                    if app_cfg.trading_enabled:
                                        from app.modules.trade_executor import execute_trade
                                        import asyncio as _aio
                                        engine.on_trade_pending()
                                        event_id = f"se-instant-{int(time.time())}"
                                        _aio.create_task(execute_trade(
                                            symbol=symbol,
                                            signal=sig["direction"],
                                            price=sig["entry_price"],
                                            event_id=event_id,
                                            tf=engine.interval,
                                        ))
                                        await log.ainfo("last_signal_executed_instant", symbol=symbol, direction=sig["direction"])
                    else:
                        async with engine._state_lock:
                            pos_side = "LONG" if side == "BUY" else "SHORT"
                            engine.on_position_opened(pos_side)
                            await log.ainfo("signal_engine_position_opened", symbol=symbol, side=pos_side)
            except Exception as e:
                await log.awarning("signal_engine_notify_error", symbol=symbol, error=str(e))

    elif event_type == "ACCOUNT_UPDATE":
        # Pozisyon degisiklikleri — loglama icin
        pass


async def _stream_loop() -> None:
    """Ana WebSocket dongusu — listenKey al, baglan, dinle, yenile."""
    global _listen_key

    await log.ainfo("order_stream_starting")

    try:
        while True:
            try:
                # ListenKey al
                _listen_key = await _get_listen_key()
                ws_url = f"{_ws_base()}{_listen_key}"
                await log.ainfo("order_stream_connected", listen_key=_listen_key[:12] + "...")

                last_renew = time.time()

                async with websockets.connect(ws_url, ping_interval=60) as ws:
                    while True:
                        # ListenKey yenileme kontrolu
                        if time.time() - last_renew > LISTEN_KEY_RENEW_INTERVAL:
                            try:
                                await _renew_listen_key(_listen_key)
                                last_renew = time.time()
                                await log.ainfo("listen_key_renewed")
                            except Exception as e:
                                await log.awarning("listen_key_renew_failed", error=str(e))

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=60)
                            data = json.loads(raw)
                            await _handle_event(data)
                        except asyncio.TimeoutError:
                            # 60sn mesaj gelmedi — normal, keepalive
                            continue

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await log.awarning("order_stream_disconnected", error=str(exc))
                await asyncio.sleep(RECONNECT_DELAY)

    except asyncio.CancelledError:
        await log.ainfo("order_stream_stopped")


def start_order_stream() -> asyncio.Task[None]:
    """Order stream baslat."""
    global _task
    if _task is not None and not _task.done():
        return _task
    _task = asyncio.create_task(_stream_loop())
    return _task


async def stop_order_stream() -> None:
    """Order stream durdur."""
    global _task
    if _task is not None and not _task.done():
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        _task = None
