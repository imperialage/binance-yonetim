"""Real-time price stream from Binance Futures.

Primary: WebSocket `!miniTicker@arr`
Fallback: REST API polling (1s interval) if WebSocket fails.
"""

from __future__ import annotations

import asyncio
from typing import Any

from app.utils.logging import get_logger

log = get_logger(__name__)

BINANCE_WS_URL = "wss://fstream.binance.com/ws/!miniTicker@arr"
BINANCE_REST_URL = "https://fapi.binance.com/fapi/v1/ticker/price"
RECONNECT_DELAY = 3
REST_POLL_INTERVAL = 1  # seconds

_prices: dict[str, float] = {}
_task: asyncio.Task[None] | None = None


def get_live_price(symbol: str) -> float | None:
    """Return the latest price for *symbol* from memory, or None."""
    return _prices.get(symbol.upper())


def get_all_prices() -> dict[str, float]:
    """Return a shallow copy of all live prices."""
    return dict(_prices)


async def _ws_stream() -> bool:
    """Try WebSocket stream. Returns False if it fails to connect."""
    try:
        import websockets
        async with websockets.connect(BINANCE_WS_URL) as ws:
            # 10sn icerisinde veri gelmezse fallback'e gec
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
            except asyncio.TimeoutError:
                log.warning("price_stream_ws_no_data")
                return False

            log.info("price_stream_ws_connected")
            # Ilk veriyi isle
            data: list[dict[str, Any]] = __import__("json").loads(raw)
            for ticker in data:
                s = ticker.get("s")
                p = ticker.get("c")
                if s and p:
                    _prices[s] = float(p)

            # Devam et
            async for raw in ws:
                data = __import__("json").loads(raw)
                for ticker in data:
                    s = ticker.get("s")
                    p = ticker.get("c")
                    if s and p:
                        _prices[s] = float(p)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning("price_stream_ws_failed", error=str(exc))
        return False
    return True


async def _rest_poll() -> None:
    """REST API polling fallback — 1sn arayla tum fiyatlari cek."""
    import httpx
    log.info("price_stream_rest_polling_started")
    async with httpx.AsyncClient(timeout=5) as client:
        while True:
            try:
                resp = await client.get(BINANCE_REST_URL)
                if resp.is_success:
                    data = resp.json()
                    for ticker in data:
                        s = ticker.get("symbol", "")
                        p = ticker.get("price", "")
                        if s and p:
                            _prices[s] = float(p)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("price_stream_rest_error", error=str(exc))
            await asyncio.sleep(REST_POLL_INTERVAL)


async def _stream_loop() -> None:
    """Connect to Binance — WebSocket first, REST fallback."""
    log.info("price_stream_started")
    try:
        while True:
            # WebSocket dene
            ws_ok = await _ws_stream()
            if not ws_ok:
                # WebSocket basarisiz — REST polling'e gec
                log.info("price_stream_fallback_to_rest")
                await _rest_poll()
            # ws_stream normal ciktiysa (disconnect) tekrar dene
            await asyncio.sleep(RECONNECT_DELAY)
    except asyncio.CancelledError:
        log.info("price_stream_stopped")


def start_price_stream() -> asyncio.Task[None]:
    global _task
    _task = asyncio.create_task(_stream_loop())
    return _task


async def stop_price_stream() -> None:
    global _task
    if _task is not None and not _task.done():
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        _task = None
