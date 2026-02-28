"""Real-time price stream from Binance Futures WebSocket.

Connects to Binance `!miniTicker@arr` stream and keeps prices in memory.
Follows the same background-task lifecycle pattern as scheduler.py.
"""

from __future__ import annotations

import asyncio
from typing import Any

import websockets

from app.utils.logging import get_logger

log = get_logger(__name__)

BINANCE_WS_URL = "wss://fstream.binance.com/ws/!miniTicker@arr"
RECONNECT_DELAY = 3  # seconds

_prices: dict[str, float] = {}
_task: asyncio.Task[None] | None = None


def get_live_price(symbol: str) -> float | None:
    """Return the latest price for *symbol* from memory, or None."""
    return _prices.get(symbol.upper())


def get_all_prices() -> dict[str, float]:
    """Return a shallow copy of all live prices."""
    return dict(_prices)


async def _stream_loop() -> None:
    """Connect to Binance WS and update prices forever (until cancelled)."""
    log.info("price_stream_started")
    try:
        while True:
            try:
                async with websockets.connect(BINANCE_WS_URL) as ws:
                    log.info("price_stream_connected")
                    async for raw in ws:
                        data: list[dict[str, Any]] = __import__("json").loads(raw)
                        for ticker in data:
                            symbol = ticker.get("s")
                            price = ticker.get("c")  # close price
                            if symbol and price:
                                _prices[symbol] = float(price)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("price_stream_disconnected", error=str(exc))
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
