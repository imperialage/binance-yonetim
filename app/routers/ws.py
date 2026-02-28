"""WebSocket endpoint for real-time price streaming to clients."""

from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.modules.price_stream import get_all_prices
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()

_clients: set[WebSocket] = set()


@router.websocket("/ws/prices")
async def prices_ws(ws: WebSocket) -> None:
    await ws.accept()
    _clients.add(ws)
    log.info("ws_client_connected", clients=len(_clients))
    try:
        while True:
            prices = get_all_prices()
            await ws.send_text(json.dumps(prices))
            await asyncio.sleep(1)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        _clients.discard(ws)
        log.info("ws_client_disconnected", clients=len(_clients))
