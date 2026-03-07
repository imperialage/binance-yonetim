"""Backtest API endpoints – Geçmiş performans analizi."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.modules.backtest_engine import (
    fetch_klines_range,
    find_optimal_sl_tp,
    get_all_futures_symbols,
    run_backtest,
)
from app.modules.supertrend import calculate_adaptive_supertrend
from app.utils.logging import get_logger

log = get_logger(__name__)

router = APIRouter(prefix="/api/backtest", tags=["backtest"])

VALID_INTERVALS = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "1d"}
MAX_DAYS = 90


class BacktestRequest(BaseModel):
    symbol: str
    interval: str = "5m"
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD
    tp_pct: float = 1.0
    sl_pct: float = 1.0


def _parse_dates(start_date: str, end_date: str) -> tuple[int, int]:
    """Tarih string'lerini milisaniye timestamp'e çevir."""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc
        )
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Tarih formatı YYYY-MM-DD olmalı"
        )

    if start_dt >= end_dt:
        raise HTTPException(
            status_code=400, detail="Başlangıç tarihi bitiş tarihinden önce olmalı"
        )

    diff_days = (end_dt - start_dt).days
    if diff_days > MAX_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"Maksimum {MAX_DAYS} günlük aralık desteklenir",
        )

    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


@router.get("/symbols")
async def get_symbols():
    """Binance Futures USDT-M perpetual sembol listesi."""
    try:
        symbols = await get_all_futures_symbols()
        return {"symbols": symbols, "count": len(symbols)}
    except Exception as e:
        log.error("symbols_fetch_error", error=str(e))
        raise HTTPException(status_code=502, detail="Sembol listesi alınamadı")


@router.get("/chart/{symbol}")
async def get_backtest_chart(
    symbol: str,
    interval: str = Query("5m"),
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
):
    """Tarih aralığı için mum + supertrend verisi (warmup trimlenmiş)."""
    symbol = symbol.upper()
    if interval not in VALID_INTERVALS:
        raise HTTPException(status_code=400, detail=f"Geçersiz interval: {interval}")

    start_ms, end_ms = _parse_dates(start_date, end_date)

    try:
        klines = await fetch_klines_range(symbol, interval, start_ms, end_ms)
    except Exception as e:
        log.error("backtest_chart_error", symbol=symbol, error=str(e))
        raise HTTPException(status_code=502, detail="Kline verisi alınamadı")

    if not klines:
        raise HTTPException(status_code=404, detail="Veri bulunamadı")

    candles, supertrend = calculate_adaptive_supertrend(klines)

    # Warmup trimle: sadece start_ms sonrası
    start_sec = start_ms // 1000
    trim_idx = 0
    for i, c in enumerate(candles):
        if c["time"] >= start_sec:
            trim_idx = i
            break

    candles = candles[trim_idx:]
    supertrend = supertrend[trim_idx:]

    return {
        "symbol": symbol,
        "interval": interval,
        "candles": candles,
        "supertrend": supertrend,
    }


@router.post("/run")
async def run_backtest_endpoint(req: BacktestRequest):
    """Backtest çalıştır – summary + trades + daily_summary."""
    symbol = req.symbol.upper()
    if req.interval not in VALID_INTERVALS:
        raise HTTPException(
            status_code=400, detail=f"Geçersiz interval: {req.interval}"
        )
    if req.tp_pct <= 0 or req.sl_pct <= 0:
        raise HTTPException(status_code=400, detail="TP ve SL sıfırdan büyük olmalı")

    start_ms, end_ms = _parse_dates(req.start_date, req.end_date)

    try:
        klines = await fetch_klines_range(symbol, req.interval, start_ms, end_ms)
    except Exception as e:
        log.error("backtest_run_error", symbol=symbol, error=str(e))
        raise HTTPException(status_code=502, detail="Kline verisi alınamadı")

    if not klines:
        raise HTTPException(status_code=404, detail="Veri bulunamadı")

    result = run_backtest(klines, req.tp_pct, req.sl_pct, start_ms, interval=req.interval)

    return {
        "symbol": symbol,
        "interval": req.interval,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "tp_pct": req.tp_pct,
        "sl_pct": req.sl_pct,
        **result,
    }


@router.post("/optimize")
async def optimize_sl_tp(req: BacktestRequest):
    """Grid search ile optimal SL/TP bul."""
    symbol = req.symbol.upper()
    if req.interval not in VALID_INTERVALS:
        raise HTTPException(
            status_code=400, detail=f"Geçersiz interval: {req.interval}"
        )

    start_ms, end_ms = _parse_dates(req.start_date, req.end_date)

    try:
        klines = await fetch_klines_range(symbol, req.interval, start_ms, end_ms)
    except Exception as e:
        log.error("backtest_optimize_error", symbol=symbol, error=str(e))
        raise HTTPException(status_code=502, detail="Kline verisi alınamadı")

    if not klines:
        raise HTTPException(status_code=404, detail="Veri bulunamadı")

    result = find_optimal_sl_tp(klines, start_ms, interval=req.interval)

    return {
        "symbol": symbol,
        "interval": req.interval,
        "start_date": req.start_date,
        "end_date": req.end_date,
        **result,
    }
