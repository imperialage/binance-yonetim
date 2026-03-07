"""Chart data API – Kline + Adaptive Supertrend verisi."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.modules.market_data import _fetch_klines
from app.modules.supertrend import calculate_adaptive_supertrend
from app.utils.logging import get_logger

log = get_logger(__name__)

router = APIRouter(prefix="/api/chart", tags=["chart"])

VALID_INTERVALS = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "1d"}


@router.get("/{symbol}")
async def get_chart_data(
    symbol: str,
    interval: str = Query("5m", description="Kline interval"),
    limit: int = Query(300, ge=50, le=1000, description="Number of candles"),
):
    """Return candle + adaptive supertrend data for a symbol."""
    symbol = symbol.upper()
    if interval not in VALID_INTERVALS:
        raise HTTPException(status_code=400, detail=f"Invalid interval: {interval}")

    try:
        # Fetch extra candles for ATR warm-up (training_period=100 + atr_len=10)
        fetch_limit = min(limit + 110, 1000)
        klines = await _fetch_klines(symbol, interval, limit=fetch_limit)
    except Exception as e:
        log.error("chart_klines_error", symbol=symbol, error=str(e))
        raise HTTPException(status_code=502, detail="Failed to fetch kline data")

    if not klines:
        raise HTTPException(status_code=404, detail="No kline data available")

    candles, supertrend = calculate_adaptive_supertrend(klines)

    # Trim to requested limit (remove warm-up period)
    if len(candles) > limit:
        candles = candles[-limit:]
        supertrend = supertrend[-limit:]

    return {
        "symbol": symbol,
        "interval": interval,
        "candles": candles,
        "supertrend": supertrend,
    }
