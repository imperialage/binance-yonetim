"""GET /status – health check endpoint."""

from __future__ import annotations

import time

import httpx
from fastapi import APIRouter
from pydantic import BaseModel

from app.modules.redis_client import get_redis, redis_ping
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()


class StatusResponse(BaseModel):
    status: str
    redis_ok: bool
    events_last_minute: int
    uptime_seconds: int


_start_time = time.time()


@router.get("/status", response_model=StatusResponse)
async def status() -> StatusResponse:
    redis_ok = await redis_ping()

    # Count events in the last minute across known rate buckets
    events_count = 0
    try:
        r = await get_redis()
        bucket = int(time.time()) // 10
        # Scan recent rate keys (last 6 buckets = ~60 seconds)
        for offset in range(6):
            pattern = f"tv:rate:*:{bucket - offset}"
            async for key in r.scan_iter(match=pattern, count=100):
                val = await r.get(key)
                if val:
                    events_count += int(val)
    except Exception:
        pass

    return StatusResponse(
        status="ok" if redis_ok else "degraded",
        redis_ok=redis_ok,
        events_last_minute=events_count,
        uptime_seconds=int(time.time() - _start_time),
    )


@router.get("/server-ip")
async def server_ip() -> dict:
    """Return the server's outbound public IP address."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get("https://api.ipify.org")
        return {"ip": resp.text.strip()}


@router.get("/debug/proxy")
async def debug_proxy() -> dict:
    """Test Binance API connection through proxy."""
    from app.config import settings
    result: dict = {
        "proxy_configured": bool(settings.binance_proxy_url),
        "proxy_url": settings.binance_proxy_url[:30] + "..." if settings.binance_proxy_url else "",
        "trading_enabled": settings.trading_enabled,
    }
    import traceback
    try:
        from app.modules.binance_client import close_client, get_client
        await close_client()  # Force fresh client
        client = await get_client()
        resp = await client.get("/fapi/v1/premiumIndex", params={"symbol": "ETHUSDT"})
        resp.raise_for_status()
        data = resp.json()
        result["binance_ok"] = True
        result["mark_price"] = data.get("markPrice")
    except Exception as e:
        result["binance_ok"] = False
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
    return result


@router.get("/debug/trade-test")
async def debug_trade_test() -> dict:
    """Test a full signed Binance API call (balance check)."""
    import traceback
    result: dict = {}
    try:
        from app.modules.binance_client import get_usdt_balance, get_position_risk
        balance = await get_usdt_balance()
        result["balance"] = balance
        positions = await get_position_risk("ETHUSDT")
        for p in positions:
            if p.get("symbol") == "ETHUSDT":
                result["position_amt"] = p.get("positionAmt")
                break
        result["ok"] = True
    except Exception as e:
        result["ok"] = False
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
    return result


@router.get("/debug/diagnosis")
async def debug_diagnosis() -> dict:
    """Full system diagnosis: config, direction filters, recent trades, positions."""
    from app.config import settings
    from app.modules.redis_client import get_redis
    from app.modules.trade_store import query_trades

    result: dict = {
        "config": {
            "trading_enabled": settings.trading_enabled,
            "binance_testnet": settings.binance_testnet,
            "stop_loss_pct": settings.stop_loss_pct,
            "take_profit_pct": settings.take_profit_pct,
            "proxy_configured": bool(settings.binance_proxy_url),
            "trading_symbols": settings.trading_symbols,
            "strategies": {
                "1m": {"sl_pct": settings.strategy_1m_sl_pct, "tp_pct": settings.strategy_1m_tp_pct},
                "5m": {"sl_pct": settings.strategy_5m_sl_pct, "tp_pct": settings.strategy_5m_tp_pct},
            },
        },
    }

    # Direction filter state from Redis
    try:
        r = await get_redis()
        direction_keys: list[dict] = []
        async for key in r.scan_iter(match="tv:signal_dir:*", count=200):
            val = await r.get(key)
            ttl = await r.ttl(key)
            direction_keys.append({
                "key": key if isinstance(key, str) else key.decode(),
                "value": val if isinstance(val, str) else (val.decode() if val else None),
                "ttl_seconds": ttl,
            })
        result["direction_filters"] = direction_keys
    except Exception as e:
        result["direction_filters_error"] = str(e)

    # Recent trades from SQLite
    try:
        trades = await query_trades(limit=10)
        result["recent_trades"] = trades
    except Exception as e:
        result["recent_trades_error"] = str(e)

    # Current Binance position
    try:
        from app.modules.binance_client import get_usdt_balance, get_position_risk
        balance = await get_usdt_balance()
        result["balance"] = balance
        positions = await get_position_risk("ETHUSDT")
        for p in positions:
            if p.get("symbol") == "ETHUSDT":
                result["position"] = {
                    "positionAmt": p.get("positionAmt"),
                    "entryPrice": p.get("entryPrice"),
                    "unRealizedProfit": p.get("unRealizedProfit"),
                    "markPrice": p.get("markPrice"),
                }
                break
    except Exception as e:
        result["binance_error"] = str(e)

    return result


@router.get("/debug/income")
async def debug_income() -> dict:
    """Get realized PnL history (last 7 days) from Binance."""
    from app.modules.binance_client import get_income_history
    from datetime import datetime, timezone, timedelta
    tz = timezone(timedelta(hours=3))
    try:
        records = await get_income_history("ETHUSDT", "REALIZED_PNL", days=7)
        trades = []
        total_pnl = 0.0
        for r in records:
            pnl = float(r.get("income", 0))
            if pnl == 0:
                continue
            total_pnl += pnl
            ts_ms = int(r.get("time", 0))
            ts_human = datetime.fromtimestamp(ts_ms / 1000, tz=tz).strftime("%Y-%m-%d %H:%M:%S")
            trades.append({
                "time": ts_human,
                "pnl": round(pnl, 6),
                "symbol": r.get("symbol"),
                "info": r.get("info", ""),
            })
        # En yenisi üstte
        trades.reverse()
        win = sum(1 for t in trades if t["pnl"] > 0)
        lose = sum(1 for t in trades if t["pnl"] < 0)
        return {
            "total_pnl": round(total_pnl, 6),
            "trade_count": len(trades),
            "win": win,
            "lose": lose,
            "win_rate": f"{(win / len(trades) * 100):.0f}%" if trades else "0%",
            "trades": trades,
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/debug/reset-directions")
async def debug_reset_directions() -> dict:
    """Reset all direction filter keys so next signal of any direction passes through."""
    from app.modules.redis_client import get_redis
    r = await get_redis()
    deleted = 0
    async for key in r.scan_iter(match="tv:signal_dir:*", count=200):
        await r.delete(key)
        deleted += 1
    return {"deleted_keys": deleted, "message": "Direction filters reset. Next signal will pass through."}
