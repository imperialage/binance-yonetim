"""Gunluk performans ozeti — her gece 00:00'da trade_closures'tan hesapla."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone, timedelta

from app.modules.trade_store import query_closures, upsert_daily_metrics
from app.utils.logging import get_logger

log = get_logger(__name__)

_task: asyncio.Task[None] | None = None
_TZ_IST = timezone(timedelta(hours=3))


async def compute_daily_metrics(date_str: str) -> dict:
    """Belirli bir gun icin trade_closures'tan metrik hesapla.

    date_str: YYYY-MM-DD formatinda (Istanbul saati).
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=_TZ_IST)
    day_start = int(dt.timestamp())
    day_end = day_start + 86400

    closures = await query_closures(after=day_start, before=day_end, limit=5000)

    total = len(closures)
    if total == 0:
        return {
            "total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0,
            "total_pnl_usdt": 0, "total_pnl_pct": 0, "avg_hold_seconds": 0,
            "tp_count": 0, "sl_count": 0, "manual_count": 0,
            "best_pnl_pct": None, "worst_pnl_pct": None,
        }

    wins = [c for c in closures if c["pnl_pct"] > 0]
    losses = [c for c in closures if c["pnl_pct"] <= 0]
    pnls = [c["pnl_pct"] for c in closures]
    hold_secs = [c["hold_duration_seconds"] for c in closures if c["hold_duration_seconds"] > 0]

    tp_count = sum(1 for c in closures if c["exit_reason"] == "TP")
    sl_count = sum(1 for c in closures if c["exit_reason"] == "SL")
    manual_count = sum(1 for c in closures if c["exit_reason"] in ("MANUAL", "REVERSE_SIGNAL", "WATCHDOG"))

    return {
        "total_trades": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / total * 100, 1) if total > 0 else 0,
        "total_pnl_usdt": round(sum(c["pnl_usdt"] for c in closures), 2),
        "total_pnl_pct": round(sum(pnls), 4),
        "avg_hold_seconds": round(sum(hold_secs) / len(hold_secs)) if hold_secs else 0,
        "tp_count": tp_count,
        "sl_count": sl_count,
        "manual_count": manual_count,
        "best_pnl_pct": round(max(pnls), 4) if pnls else None,
        "worst_pnl_pct": round(min(pnls), 4) if pnls else None,
    }


async def _daily_loop() -> None:
    """Her gece 00:00 Istanbul'da onceki gunun metriklerini hesapla."""
    await log.ainfo("daily_metrics_updater_started")
    try:
        while True:
            now_ist = datetime.now(_TZ_IST)
            # Bir sonraki 00:05'e kadar bekle (5dk marja)
            tomorrow = (now_ist + timedelta(days=1)).replace(hour=0, minute=5, second=0, microsecond=0)
            wait_seconds = (tomorrow - now_ist).total_seconds()
            await asyncio.sleep(wait_seconds)

            # Dunku gunu hesapla
            yesterday = (datetime.now(_TZ_IST) - timedelta(days=1)).strftime("%Y-%m-%d")
            try:
                metrics = await compute_daily_metrics(yesterday)
                await upsert_daily_metrics(yesterday, metrics)
                await log.ainfo("daily_metrics_computed", date=yesterday, trades=metrics["total_trades"])
            except Exception as e:
                await log.aerror("daily_metrics_failed", date=yesterday, error=str(e))

    except asyncio.CancelledError:
        await log.ainfo("daily_metrics_updater_stopped")


def start_daily_metrics_updater() -> asyncio.Task[None]:
    global _task
    if _task is not None and not _task.done():
        return _task
    _task = asyncio.create_task(_daily_loop())
    return _task


async def stop_daily_metrics_updater() -> None:
    global _task
    if _task is not None and not _task.done():
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        _task = None
