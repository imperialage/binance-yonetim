"""Data Collector API – Mum verisi toplama ve SuperTrend sinyal kayıt endpoint'leri."""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.modules.candle_store import (
    get_candle_stats,
    get_last_open_time,
    query_candles,
    upsert_candles,
)
from app.modules.data_collector import (
    compute_signals,
    fetch_all_klines,
    get_all_status,
    start_collection,
    stop_collection,
    INTERVAL_MS,
)
from app.utils.logging import get_logger

log = get_logger(__name__)

router = APIRouter(prefix="/api/collector", tags=["data-collector"])


class CollectionRequest(BaseModel):
    symbol: str = "ETHUSDT"
    interval: str = "5m"


class FetchHistoryRequest(BaseModel):
    symbol: str = "ETHUSDT"
    interval: str = "5m"
    days: int = 730  # Varsayılan: 2 yıl (mümkün olan maksimum)


# ── Toplama kontrolü ──


@router.post("/start")
async def start_collector(req: CollectionRequest):
    """Sürekli veri toplama başlat."""
    symbol = req.symbol.upper()
    interval = req.interval
    if interval not in INTERVAL_MS:
        raise HTTPException(status_code=400, detail=f"Geçersiz interval: {interval}")

    result = start_collection(symbol, interval)
    return result


@router.post("/stop")
async def stop_collector(req: CollectionRequest):
    """Veri toplamayı durdur."""
    symbol = req.symbol.upper()
    result = stop_collection(symbol, req.interval)
    return result


@router.get("/status")
async def collector_status():
    """Tüm aktif koleksiyon durumları."""
    status = get_all_status()
    return {"collectors": status}


# ── Geçmiş veri çekme (tek seferlik) ──


@router.post("/fetch-history")
async def fetch_history(req: FetchHistoryRequest):
    """Geçmiş veriyi çek, SuperTrend hesapla, DB'ye kaydet.

    DB'de zaten veri varsa sadece eksik kısmı çeker.
    """
    symbol = req.symbol.upper()
    interval = req.interval
    days = min(req.days, 730)

    if interval not in INTERVAL_MS:
        raise HTTPException(status_code=400, detail=f"Geçersiz interval: {interval}")

    iv_ms = INTERVAL_MS[interval]
    now_ms = int(time.time() * 1000)

    # İstenen başlangıç zamanı
    desired_start_ms = now_ms - (days * 86400 * 1000)

    # SuperTrend warmup: 2000 mum öncesinden çek
    warmup_ms = 2000 * iv_ms
    fetch_start_ms = desired_start_ms - warmup_ms

    try:
        klines = await fetch_all_klines(symbol, interval, fetch_start_ms, now_ms)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Kline verisi alınamadı: {e}")

    if not klines:
        raise HTTPException(status_code=404, detail="Veri bulunamadı")

    rows = compute_signals(klines)

    # Tüm hesaplanmış satırları DB'ye yaz (upsert — duplicate sorun olmaz)
    count = await upsert_candles(rows, symbol, interval)
    stats = await get_candle_stats(symbol, interval)

    # Geçmiş çekildikten sonra otomatik olarak sürekli toplamayı başlat
    start_collection(symbol, interval)

    return {
        "symbol": symbol,
        "interval": interval,
        "days": days,
        "total_candles": stats["rows"],
        "total_signals": stats["total_signals"],
        "buy_signals": stats["buy_signals"],
        "sell_signals": stats["sell_signals"],
        "first_date": stats.get("first_date"),
        "last_date": stats.get("last_date"),
    }


# ── Veri okuma ──


@router.get("/data/{symbol}")
async def get_data(
    symbol: str,
    interval: str = Query("5m"),
    limit: int = Query(200, ge=1, le=5000),
    signals_only: bool = Query(False),
    date_from: str | None = Query(None, description="Başlangıç tarihi (YYYY-MM-DD)"),
    date_to: str | None = Query(None, description="Bitiş tarihi (YYYY-MM-DD)"),
):
    """DB'den son N mumu oku. Opsiyonel tarih filtresi."""
    symbol = symbol.upper()
    stats = await get_candle_stats(symbol, interval)

    if not stats["exists"]:
        raise HTTPException(
            status_code=404,
            detail=f"{symbol} {interval} verisi bulunamadı. Önce veri çekin.",
        )

    rows = await query_candles(
        symbol, interval, limit=limit, signals_only=signals_only,
        date_from=date_from, date_to=date_to,
    )

    return {
        "symbol": symbol,
        "interval": interval,
        "stats": stats,
        "data": rows,
    }


@router.get("/stats/{symbol}")
async def get_stats(symbol: str, interval: str = Query("5m")):
    """Mum verisi istatistikleri."""
    symbol = symbol.upper()
    stats = await get_candle_stats(symbol, interval)
    return {"symbol": symbol, "interval": interval, **stats}
