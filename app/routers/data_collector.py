"""Data Collector API – Mum verisi toplama ve SuperTrend sinyal kayıt endpoint'leri."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.modules.data_collector import (
    _csv_path,
    fetch_all_klines,
    compute_signals,
    get_all_status,
    get_csv_stats,
    read_csv_tail,
    save_to_csv,
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
    days: int = 30


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
    """Geçmiş veriyi çek, SuperTrend hesapla, CSV'ye kaydet."""
    symbol = req.symbol.upper()
    interval = req.interval
    days = min(req.days, 365)

    if interval not in INTERVAL_MS:
        raise HTTPException(status_code=400, detail=f"Geçersiz interval: {interval}")

    iv_ms = INTERVAL_MS[interval]
    end_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    # Warmup için ekstra 2000 mum
    warmup_ms = 2000 * iv_ms
    start_ms = end_ms - (days * 86400 * 1000) - warmup_ms

    try:
        klines = await fetch_all_klines(symbol, interval, start_ms, end_ms)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Kline verisi alınamadı: {e}")

    if not klines:
        raise HTTPException(status_code=404, detail="Veri bulunamadı")

    rows = compute_signals(klines)

    # Warmup kısmını kes (ilk 2000 mum sadece hesaplama içindi)
    actual_start = end_ms - (days * 86400 * 1000)
    actual_start_str = datetime.fromtimestamp(
        actual_start / 1000, tz=timezone(timedelta(hours=3))
    ).strftime("%Y-%m-%d %H:%M:%S")
    rows = [r for r in rows if r["date"] >= actual_start_str]

    filepath = _csv_path(symbol, interval)
    count = save_to_csv(rows, filepath)

    signals = [r for r in rows if r.get("signal")]

    return {
        "symbol": symbol,
        "interval": interval,
        "days": days,
        "total_candles": count,
        "total_signals": len(signals),
        "buy_signals": sum(1 for s in signals if s["signal"] == "BUY"),
        "sell_signals": sum(1 for s in signals if s["signal"] == "SELL"),
        "csv_path": str(filepath),
    }


# ── CSV veri okuma ──


@router.get("/data/{symbol}")
async def get_data(
    symbol: str,
    interval: str = Query("5m"),
    limit: int = Query(200, ge=1, le=5000),
    signals_only: bool = Query(False),
):
    """CSV'den son N satırı oku."""
    symbol = symbol.upper()
    filepath = _csv_path(symbol, interval)
    stats = get_csv_stats(filepath)

    if not stats["exists"]:
        raise HTTPException(
            status_code=404,
            detail=f"{symbol} {interval} verisi bulunamadı. Önce /fetch-history çalıştırın.",
        )

    rows = read_csv_tail(filepath, limit=limit if not signals_only else 5000)

    if signals_only:
        rows = [r for r in rows if r.get("signal")]
        rows = rows[-limit:]

    return {
        "symbol": symbol,
        "interval": interval,
        "stats": stats,
        "data": rows,
    }


@router.get("/stats/{symbol}")
async def get_stats(symbol: str, interval: str = Query("5m")):
    """CSV dosyası istatistikleri."""
    symbol = symbol.upper()
    filepath = _csv_path(symbol, interval)
    stats = get_csv_stats(filepath)
    return {"symbol": symbol, "interval": interval, **stats}


@router.get("/download/{symbol}")
async def download_csv(symbol: str, interval: str = Query("5m")):
    """CSV dosyasını indir."""
    symbol = symbol.upper()
    filepath = _csv_path(symbol, interval)
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="CSV dosyası bulunamadı")

    return FileResponse(
        str(filepath),
        media_type="text/csv",
        filename=f"{symbol}_{interval}_supertrend.csv",
    )
