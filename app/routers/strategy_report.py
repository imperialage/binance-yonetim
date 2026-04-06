"""GET /api/strategy-report — trade_closures tablosundan strateji analiz raporu."""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Query

from app.modules.trade_store import query_closures, get_post_exit_candles, query_daily_metrics
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()

_TZ_IST = timezone(timedelta(hours=3))


@router.get("/api/strategy-report")
async def strategy_report(
    days: int = Query(30, ge=1, le=365),
    symbol: str | None = Query(None),
) -> dict:
    """Strateji analiz raporu — trade_closures tablosundan."""
    now = int(time.time())
    after = now - (days * 86400)

    closures = await query_closures(symbol=symbol, after=after, limit=5000)

    if not closures:
        return {
            "period": f"last_{days}_days",
            "summary": _empty_summary(),
            "by_symbol": {},
            "by_direction": {},
            "by_exit_reason": {},
            "by_hour": {},
            "worst_trades": [],
            "best_trades": [],
            "recent_trades": [],
        }

    # ── Summary ──
    summary = _compute_summary(closures)

    # ── By Symbol ──
    by_symbol: dict[str, dict] = {}
    symbol_groups: dict[str, list] = {}
    for c in closures:
        s = c["symbol"]
        symbol_groups.setdefault(s, []).append(c)
    for s, group in symbol_groups.items():
        by_symbol[s] = _compute_summary(group)

    # ── By Direction ──
    by_direction: dict[str, dict] = {}
    dir_groups: dict[str, list] = {}
    for c in closures:
        d = c["direction"]
        dir_groups.setdefault(d, []).append(c)
    for d, group in dir_groups.items():
        by_direction[d] = _compute_summary(group)

    # ── By Exit Reason ──
    by_exit_reason: dict[str, dict] = {}
    reason_groups: dict[str, list] = {}
    for c in closures:
        r = c["exit_reason"]
        reason_groups.setdefault(r, []).append(c)
    for r, group in reason_groups.items():
        pnls = [c["pnl_pct"] for c in group]
        by_exit_reason[r] = {
            "count": len(group),
            "avg_pnl": round(sum(pnls) / len(pnls), 4) if pnls else 0,
            "total_pnl": round(sum(pnls), 4),
        }

    # ── By Hour ──
    by_hour: dict[str, dict] = {}
    hour_groups: dict[int, list] = {}
    for c in closures:
        h = datetime.fromtimestamp(c["entry_time"], tz=_TZ_IST).hour if c["entry_time"] > 0 else 0
        hour_groups.setdefault(h, []).append(c)
    for h in range(24):
        group = hour_groups.get(h, [])
        if group:
            wins = [c for c in group if c["pnl_pct"] > 0]
            by_hour[str(h)] = {
                "trades": len(group),
                "win_rate": round(len(wins) / len(group) * 100, 1),
                "avg_pnl": round(sum(c["pnl_pct"] for c in group) / len(group), 4),
            }
        else:
            by_hour[str(h)] = {"trades": 0, "win_rate": 0, "avg_pnl": 0}

    # ── Best / Worst / Recent ──
    sorted_by_pnl = sorted(closures, key=lambda c: c["pnl_pct"])
    worst_trades = [_format_trade(c) for c in sorted_by_pnl[:5]]
    best_trades = [_format_trade(c) for c in sorted_by_pnl[-5:][::-1]]
    recent_trades = [_format_trade(c) for c in closures[:20]]

    return {
        "period": f"last_{days}_days",
        "summary": summary,
        "by_symbol": by_symbol,
        "by_direction": by_direction,
        "by_exit_reason": by_exit_reason,
        "by_hour": by_hour,
        "worst_trades": worst_trades,
        "best_trades": best_trades,
        "recent_trades": recent_trades,
    }


@router.get("/api/strategy-report/post-exit/{closure_id}")
async def post_exit_detail(closure_id: int) -> dict:
    """Tek bir trade'in SL sonrasi mum verileri."""
    candles = await get_post_exit_candles(closure_id)
    return {"closure_id": closure_id, "candles": candles, "count": len(candles)}


@router.get("/api/strategy-report/post-exit-summary")
async def post_exit_summary(days: int = Query(30, ge=1, le=365)) -> dict:
    """SL kapanislarinin post-exit analizi — SL cok mu dar?"""
    now = int(time.time())
    after = now - (days * 86400)
    closures = await query_closures(exit_reason="SL", after=after, limit=1000)

    results = []
    for c in closures:
        candles = await get_post_exit_candles(c["id"])
        if not candles:
            continue
        direction = c["direction"]
        exit_price = c["exit_price"]
        # Fiyat SL sonrasi geri geldi mi?
        if direction == "LONG":
            # SL'den sonra fiyat yukseldi mi?
            max_recovery = max((cd["high"] - exit_price) / exit_price * 100 for cd in candles) if candles else 0
            max_further_drop = min((cd["low"] - exit_price) / exit_price * 100 for cd in candles) if candles else 0
        else:
            max_recovery = max((exit_price - cd["low"]) / exit_price * 100 for cd in candles) if candles else 0
            max_further_drop = min((exit_price - cd["high"]) / exit_price * 100 for cd in candles) if candles else 0

        came_back = max_recovery > abs(c["pnl_pct"])  # SL kaybini geri alacak kadar geldi mi
        results.append({
            "closure_id": c["id"],
            "symbol": c["symbol"],
            "direction": direction,
            "sl_pnl_pct": c["pnl_pct"],
            "max_recovery_pct": round(max_recovery, 4),
            "max_further_drop_pct": round(max_further_drop, 4),
            "came_back": came_back,
            "candle_count": len(candles),
        })

    came_back_count = sum(1 for r in results if r["came_back"])
    total = len(results)

    return {
        "period": f"last_{days}_days",
        "total_sl_with_data": total,
        "came_back_count": came_back_count,
        "came_back_pct": round(came_back_count / total * 100, 1) if total > 0 else 0,
        "recommendation": "SL cok dar — genisletmeyi dusun" if total > 0 and came_back_count / total > 0.5 else "SL uygun",
        "details": results,
    }


@router.get("/api/strategy-report/daily")
async def daily_metrics_endpoint(days: int = Query(30, ge=1, le=365)) -> dict:
    """Gunluk performans metrikleri."""
    metrics = await query_daily_metrics(days)
    return {"days": days, "metrics": metrics, "count": len(metrics)}


def _compute_summary(closures: list[dict]) -> dict:
    total = len(closures)
    if total == 0:
        return _empty_summary()

    wins = [c for c in closures if c["pnl_pct"] > 0]
    losses = [c for c in closures if c["pnl_pct"] <= 0]
    win_pcts = [c["pnl_pct"] for c in wins]
    loss_pcts = [c["pnl_pct"] for c in losses]
    all_pnls = [c["pnl_pct"] for c in closures]
    hold_secs = [c["hold_duration_seconds"] for c in closures if c["hold_duration_seconds"] > 0]

    gross_profit = sum(p for p in all_pnls if p > 0)
    gross_loss = abs(sum(p for p in all_pnls if p < 0))
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else float("inf")

    return {
        "total_trades": total,
        "win_rate": round(len(wins) / total * 100, 1),
        "avg_win_pct": round(sum(win_pcts) / len(win_pcts), 4) if win_pcts else 0,
        "avg_loss_pct": round(sum(loss_pcts) / len(loss_pcts), 4) if loss_pcts else 0,
        "total_pnl_pct": round(sum(all_pnls), 4),
        "total_pnl_usdt": round(sum(c["pnl_usdt"] for c in closures), 2),
        "profit_factor": profit_factor,
        "avg_hold_minutes": round(sum(hold_secs) / len(hold_secs) / 60, 1) if hold_secs else 0,
    }


def _empty_summary() -> dict:
    return {
        "total_trades": 0,
        "win_rate": 0,
        "avg_win_pct": 0,
        "avg_loss_pct": 0,
        "total_pnl_pct": 0,
        "total_pnl_usdt": 0,
        "profit_factor": 0,
        "avg_hold_minutes": 0,
    }


def _format_trade(c: dict) -> dict:
    entry_dt = datetime.fromtimestamp(c["entry_time"], tz=_TZ_IST).strftime("%d.%m %H:%M") if c["entry_time"] > 0 else ""
    exit_dt = datetime.fromtimestamp(c["exit_time"], tz=_TZ_IST).strftime("%d.%m %H:%M") if c["exit_time"] > 0 else ""
    return {
        "id": c["id"],
        "symbol": c["symbol"],
        "direction": c["direction"],
        "entry_price": c["entry_price"],
        "exit_price": c["exit_price"],
        "entry_time": entry_dt,
        "exit_time": exit_dt,
        "exit_reason": c["exit_reason"],
        "pnl_pct": c["pnl_pct"],
        "pnl_usdt": c["pnl_usdt"],
        "hold_minutes": round(c["hold_duration_seconds"] / 60, 1) if c["hold_duration_seconds"] > 0 else 0,
        "signal_id": c.get("signal_id"),
    }
