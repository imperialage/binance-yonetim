"""GET /latest – retrieve latest two-layer evaluation for a symbol."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from app.modules.market_data import get_last_price, get_market_summaries
from app.modules.redis_client import get_redis
from app.schemas.evaluation import LatestEvaluation

router = APIRouter()


def _ts_human(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


@router.get("/latest")
async def latest(symbol: str = Query(..., examples=["ETHUSDT"])) -> JSONResponse:
    r = await get_redis()
    raw = await r.get(f"tv:latest:{symbol.upper()}")
    if raw is None:
        return JSONResponse(
            status_code=404,
            content={"detail": f"No evaluation found for {symbol.upper()}"},
        )
    ev = LatestEvaluation.model_validate_json(raw)
    data = ev.model_dump()

    # Add human-readable timestamps
    data["evaluated_at_human"] = _ts_human(ev.evaluated_at)
    if ev.latest_ai and ev.latest_ai.generated_at:
        data["latest_ai"]["generated_at_human"] = _ts_human(ev.latest_ai.generated_at)
    for sig in data.get("latest_rules", {}).get("signals_used", []):
        if sig.get("ts"):
            sig["ts_human"] = _ts_human(sig["ts"])

    return JSONResponse(content=data)


@router.get("/price")
async def price(symbol: str = Query(..., examples=["ETHUSDT"])) -> JSONResponse:
    """Get current price and market summary from Binance Futures."""
    symbol = symbol.strip().upper()
    last = await get_last_price(symbol)
    if last == 0.0:
        return JSONResponse(status_code=404, content={"detail": f"Price not found for {symbol}"})

    summaries = await get_market_summaries(symbol)
    return JSONResponse(content={
        "symbol": symbol,
        "price": last,
        "market": {tf: s.model_dump() for tf, s in summaries.items()},
    })
