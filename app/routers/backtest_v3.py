"""Backtest v3 API — HA Renk + Prev Bar High/Low Limit stratejisi.

Endpoints:
  POST /api/backtest/v3/run  -> tek TP/SL kombinasyonu backtest
  POST /api/backtest/v3/grid -> TP × SL grid heatmap (max 200 kombinasyon)

Pine v3 mantiginin Python portu. Mum verisi Binance'tan chart TF + HTF
ayri fetch edilir. Heikin-Ashi HTF frame'de hesaplanir.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.modules.backtest_engine import fetch_klines_range
from app.modules.backtest_v3_engine import BacktestParams, run_v3_backtest, run_v3_grid
from app.utils.logging import get_logger

log = get_logger(__name__)

router = APIRouter(prefix="/api/backtest/v3", tags=["backtest_v3"])

VALID_INTERVALS = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"}
MAX_DAYS = 90
MAX_GRID_CELLS = 200  # tp_count * sl_count sınırı


class RunReq(BaseModel):
    symbol: str = Field(examples=["ETHUSDT", "MYXUSDT"])
    chart_tf: str = Field(default="15m", examples=["5m", "15m", "1h"])
    htf: str = Field(default="1h", examples=["1h", "4h"])
    start_date: str = Field(examples=["2026-06-01"])
    end_date: str = Field(examples=["2026-07-20"])
    tp_pct: float = Field(default=1.0, ge=0.05, le=10.0)
    sl_pct: float = Field(default=0.5, ge=0.05, le=10.0)
    use_good: bool = False
    good_pct: float = Field(default=0.5, ge=0.0, le=5.0)
    require_prev_htf: bool = False
    commission_pct: float = Field(default=0.08, ge=0.0, le=1.0,
                                  description="Round-trip komisyon %")


class GridReq(BaseModel):
    symbol: str
    chart_tf: str = "15m"
    htf: str = "1h"
    start_date: str
    end_date: str
    # Grid range: tp_min, tp_max, tp_step (yuzde olarak, ornek 0.3-3.0 step 0.1)
    tp_min: float = Field(default=0.3, ge=0.05, le=10.0)
    tp_max: float = Field(default=2.0, ge=0.05, le=10.0)
    tp_step: float = Field(default=0.1, gt=0.0)
    sl_min: float = Field(default=0.2, ge=0.05, le=10.0)
    sl_max: float = Field(default=2.0, ge=0.05, le=10.0)
    sl_step: float = Field(default=0.1, gt=0.0)
    use_good: bool = False
    good_pct: float = Field(default=0.5, ge=0.0, le=5.0)
    require_prev_htf: bool = False
    commission_pct: float = Field(default=0.08, ge=0.0, le=1.0)


def _parse_dates(start_date: str, end_date: str) -> tuple[int, int]:
    try:
        s = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        e = datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc
        )
    except ValueError:
        raise HTTPException(400, "Tarih formati YYYY-MM-DD olmali")
    if s >= e:
        raise HTTPException(400, "Baslangic < bitis olmali")
    if (e - s).days > MAX_DAYS:
        raise HTTPException(400, f"Maks {MAX_DAYS} gun destekli")
    return int(s.timestamp() * 1000), int(e.timestamp() * 1000)


def _validate_tf(tf: str, field: str):
    if tf not in VALID_INTERVALS:
        raise HTTPException(400, f"Gecersiz {field}: {tf}")


def _build_range(mn: float, mx: float, step: float) -> list[float]:
    if step <= 0:
        raise HTTPException(400, "step > 0 olmali")
    values = []
    v = mn
    # Floating-point epsilon
    while v <= mx + 1e-9:
        values.append(round(v, 6))
        v += step
    return values


@router.post("/run")
async def run_single(req: RunReq):
    """Tek TP/SL ile backtest — detayli trade listesi + ozet."""
    symbol = req.symbol.upper()
    _validate_tf(req.chart_tf, "chart_tf")
    _validate_tf(req.htf, "htf")
    start_ms, end_ms = _parse_dates(req.start_date, req.end_date)

    # Warmup: Chart TF icin ekstra bar (HTF 1 tam period kadar + prev)
    try:
        chart_klines = await fetch_klines_range(symbol, req.chart_tf, start_ms, end_ms,
                                                extra_warmup=100)
        htf_klines = await fetch_klines_range(symbol, req.htf, start_ms, end_ms,
                                              extra_warmup=10)
    except Exception as e:
        log.error("backtest_v3_fetch_error", symbol=symbol, error=str(e))
        raise HTTPException(502, f"Kline fetch fail: {e}")

    if not chart_klines or not htf_klines:
        raise HTTPException(404, "Veri bulunamadi")

    params = BacktestParams(
        tp_pct=req.tp_pct / 100.0,
        sl_pct=req.sl_pct / 100.0,
        use_good=req.use_good,
        good_pct=req.good_pct / 100.0,
        require_prev_htf=req.require_prev_htf,
        commission_pct=req.commission_pct / 100.0,
    )

    result = run_v3_backtest(chart_klines, htf_klines, params, start_ms)
    return {
        "symbol": symbol,
        "chart_tf": req.chart_tf,
        "htf": req.htf,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "params": {
            "tp_pct": req.tp_pct,
            "sl_pct": req.sl_pct,
            "use_good": req.use_good,
            "good_pct": req.good_pct,
            "require_prev_htf": req.require_prev_htf,
            "commission_pct": req.commission_pct,
        },
        **result,
    }


@router.post("/grid")
async def run_grid(req: GridReq):
    """TP × SL grid backtest — heatmap icin matrix + en iyi kombinasyonlar."""
    symbol = req.symbol.upper()
    _validate_tf(req.chart_tf, "chart_tf")
    _validate_tf(req.htf, "htf")
    start_ms, end_ms = _parse_dates(req.start_date, req.end_date)

    tp_range_pct = _build_range(req.tp_min, req.tp_max, req.tp_step)
    sl_range_pct = _build_range(req.sl_min, req.sl_max, req.sl_step)
    n_cells = len(tp_range_pct) * len(sl_range_pct)
    if n_cells > MAX_GRID_CELLS:
        raise HTTPException(400, f"Grid cok buyuk: {n_cells} > {MAX_GRID_CELLS}. "
                                 f"Range/step ayarlarini daralt.")
    if n_cells == 0:
        raise HTTPException(400, "Grid bos")

    try:
        chart_klines = await fetch_klines_range(symbol, req.chart_tf, start_ms, end_ms,
                                                extra_warmup=100)
        htf_klines = await fetch_klines_range(symbol, req.htf, start_ms, end_ms,
                                              extra_warmup=10)
    except Exception as e:
        log.error("backtest_v3_grid_fetch_error", symbol=symbol, error=str(e))
        raise HTTPException(502, f"Kline fetch fail: {e}")

    if not chart_klines or not htf_klines:
        raise HTTPException(404, "Veri bulunamadi")

    # Fraction'a cevir
    tp_range = [t / 100.0 for t in tp_range_pct]
    sl_range = [s / 100.0 for s in sl_range_pct]

    result = run_v3_grid(
        chart_klines=chart_klines,
        htf_klines=htf_klines,
        start_ms=start_ms,
        tp_range=tp_range,
        sl_range=sl_range,
        use_good=req.use_good,
        good_pct=req.good_pct / 100.0,
        require_prev_htf=req.require_prev_htf,
        commission_pct=req.commission_pct / 100.0,
    )

    log.info("backtest_v3_grid_done",
             symbol=symbol, n_cells=n_cells,
             chart_tf=req.chart_tf, htf=req.htf)

    return {
        "symbol": symbol,
        "chart_tf": req.chart_tf,
        "htf": req.htf,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "tp_range_pct": tp_range_pct,
        "sl_range_pct": sl_range_pct,
        "n_cells": n_cells,
        "params_common": {
            "use_good": req.use_good,
            "good_pct": req.good_pct,
            "require_prev_htf": req.require_prev_htf,
            "commission_pct": req.commission_pct,
        },
        **result,
    }
