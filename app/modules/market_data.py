"""Binance Futures klines fetcher with in-memory + optional Redis cache."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from app.schemas.evaluation import MarketSummary
from app.utils.logging import get_logger

log = get_logger(__name__)

BINANCE_FAPI = "https://fapi.binance.com/fapi/v1/klines"
CACHE_TTL = 10  # seconds
_cache: dict[str, tuple[float, list[list[Any]]]] = {}
_lock = asyncio.Lock()


async def _fetch_klines(symbol: str, interval: str, limit: int = 200) -> list[list[Any]]:
    cache_key = f"{symbol}:{interval}"

    async with _lock:
        if cache_key in _cache:
            cached_at, data = _cache[cache_key]
            if time.time() - cached_at < CACHE_TTL:
                return data

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            BINANCE_FAPI,
            params={"symbol": symbol, "interval": interval, "limit": limit},
        )
        resp.raise_for_status()
        klines = resp.json()

    async with _lock:
        _cache[cache_key] = (time.time(), klines)

    return klines


def _summarize(klines: list[list[Any]], interval: str) -> MarketSummary:
    if not klines:
        return MarketSummary(tf=interval, last_price=0, green_candles=0, red_candles=0, slope=0)

    last_20 = klines[-20:] if len(klines) >= 20 else klines
    last_price = float(klines[-1][4])  # close of last candle

    green = sum(1 for k in last_20 if float(k[4]) >= float(k[1]))  # close >= open
    red = len(last_20) - green

    first_close = float(last_20[0][4])
    last_close = float(last_20[-1][4])
    slope = last_close - first_close

    return MarketSummary(
        tf=interval,
        last_price=last_price,
        green_candles=green,
        red_candles=red,
        slope=round(slope, 4),
    )


async def get_market_summaries(symbol: str) -> dict[str, MarketSummary]:
    intervals = ["15m", "1h", "4h"]
    results: dict[str, MarketSummary] = {}

    tasks = [_fetch_klines(symbol, iv) for iv in intervals]
    klines_list = await asyncio.gather(*tasks, return_exceptions=True)

    for iv, klines_or_err in zip(intervals, klines_list):
        if isinstance(klines_or_err, Exception):
            log.error("klines_fetch_error", interval=iv, error=str(klines_or_err))
            results[iv] = MarketSummary(tf=iv, last_price=0, green_candles=0, red_candles=0, slope=0)
        else:
            results[iv] = _summarize(klines_or_err, iv)

    return results


async def get_last_price(symbol: str) -> float:
    """Quick helper: get last close from 15m klines."""
    try:
        klines = await _fetch_klines(symbol, "15m", limit=1)
        if klines:
            return float(klines[-1][4])
    except Exception as e:
        log.error("last_price_fetch_error", error=str(e))
    return 0.0
