"""Binance mum verisi toplama + Adaptive SuperTrend sinyal hesaplama modülü.

Sürekli çalışan background task ile yeni mumları çeker, SuperTrend hesaplar,
sinyalleri tespit eder ve SQLite'a kaydeder.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx

from app.modules.candle_store import (
    get_last_open_time,
    upsert_candles,
    get_candle_stats,
)
from app.modules.supertrend import calculate_adaptive_supertrend
from app.utils.logging import get_logger

log = get_logger(__name__)

BINANCE_FAPI_BASE = "https://fapi.binance.com"

# Interval → milisaniye
INTERVAL_MS: dict[str, int] = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000,
    "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000,
    "4h": 14_400_000, "1d": 86_400_000,
}

_TZ_ISTANBUL = timezone(timedelta(hours=3))

# Aktif koleksiyon görevleri
_tasks: dict[str, asyncio.Task] = {}
_status: dict[str, dict] = {}


def _format_ts(unix_sec: int) -> str:
    """Unix timestamp → Istanbul (UTC+3) string."""
    return datetime.fromtimestamp(unix_sec, tz=_TZ_ISTANBUL).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


async def fetch_all_klines(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int | None = None,
) -> list[list[Any]]:
    """Tüm kline verisini paginated olarak çek."""
    iv_ms = INTERVAL_MS.get(interval)
    if iv_ms is None:
        raise ValueError(f"Geçersiz interval: {interval}")

    if end_ms is None:
        end_ms = int(time.time() * 1000)

    all_klines: list[list[Any]] = []
    current_start = start_ms

    async with httpx.AsyncClient(timeout=15) as client:
        while current_start < end_ms:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": current_start,
                "endTime": end_ms,
                "limit": 1500,
            }
            resp = await client.get(
                f"{BINANCE_FAPI_BASE}/fapi/v1/klines", params=params
            )
            resp.raise_for_status()
            batch = resp.json()

            if not batch:
                break

            all_klines.extend(batch)
            last_open_time = int(batch[-1][0])
            current_start = last_open_time + iv_ms

            if len(batch) < 1500:
                break

            await asyncio.sleep(0.15)  # Rate limit koruması

    # Duplicate temizle
    seen: set[int] = set()
    unique: list[list[Any]] = []
    for k in all_klines:
        t = int(k[0])
        if t not in seen:
            seen.add(t)
            unique.append(k)
    unique.sort(key=lambda k: int(k[0]))
    return unique


def compute_signals(
    klines: list[list[Any]],
    atr_len: int = 10,
    factor: float = 3.0,
) -> list[dict]:
    """Kline verisinden SuperTrend + sinyal hesapla, DB satırları döndür."""
    if len(klines) < 2:
        return []

    candles, st_data = calculate_adaptive_supertrend(
        klines, atr_len=atr_len, factor=factor
    )

    rows: list[dict] = []
    for i in range(len(candles)):
        c = candles[i]
        st = st_data[i]

        # Sinyal: direction değişimi
        signal = ""
        if i > 0:
            prev_dir = st_data[i - 1]["direction"]
            curr_dir = st["direction"]
            if prev_dir != curr_dir:
                signal = "BUY" if curr_dir == -1 else "SELL"

        cluster_label = {0: "HIGH", 1: "MID", 2: "LOW"}.get(st["cluster"], "MID")

        rows.append({
            "open_time": c["time"] * 1000,  # unix ms (Binance formatı)
            "date": _format_ts(c["time"]),
            "open": c["open"],
            "high": c["high"],
            "low": c["low"],
            "close": c["close"],
            "volume": c["volume"],
            "supertrend": st["value"],
            "direction": "UP" if st["direction"] == -1 else "DOWN",
            "cluster": cluster_label,
            "signal": signal,
        })

    return rows


# ── Background collection task ──


async def _collection_loop(symbol: str, interval: str):
    """Sürekli çalışan veri toplama döngüsü."""
    iv_ms = INTERVAL_MS[interval]
    key = f"{symbol}_{interval}"

    _status[key] = {
        "symbol": symbol,
        "interval": interval,
        "running": True,
        "last_update": None,
        "total_rows": 0,
        "error": None,
    }

    await log.ainfo("data_collector_started", symbol=symbol, interval=interval)

    try:
        # DB'deki en son mum zamanını bul
        last_ms = await get_last_open_time(symbol, interval)

        if last_ms is None:
            # DB boş → Binance'ın izin verdiği en eskiden başla
            # Binance Futures genelde ~2 yıl veri tutar
            # 5m için: 2 yıl ≈ 210.000 mum
            last_ms = int(time.time() * 1000) - (730 * 86400 * 1000)  # 2 yıl geriden

        # SuperTrend warmup için 2000 mum öncesinden çekmeliyiz
        warmup_start = last_ms - (2000 * iv_ms)
        now_ms = int(time.time() * 1000)

        await log.ainfo(
            "data_collector_fetching",
            symbol=symbol,
            interval=interval,
            from_ms=warmup_start,
        )

        klines = await fetch_all_klines(symbol, interval, warmup_start, now_ms)

        if klines:
            rows = compute_signals(klines)
            # Warmup mumlarını da dahil et (SuperTrend doğruluğu için)
            # DB upsert kullanıyor, duplicate sorun olmaz
            count = await upsert_candles(rows, symbol, interval)
            stats = await get_candle_stats(symbol, interval)
            _status[key]["total_rows"] = stats["rows"]
            _status[key]["last_update"] = _format_ts(int(time.time()))

        await log.ainfo(
            "data_collector_initial_fetch",
            symbol=symbol,
            rows=_status[key]["total_rows"],
        )

        # Sürekli güncelleme döngüsü
        while True:
            # Bir sonraki mum kapanışını bekle
            now_ms = int(time.time() * 1000)
            current_candle_start = (now_ms // iv_ms) * iv_ms
            next_candle_close = current_candle_start + iv_ms
            wait_sec = max(5, (next_candle_close - now_ms) / 1000 + 3)  # +3sn buffer

            await asyncio.sleep(wait_sec)

            try:
                # Son 2100 mumu çek (warmup + yeni veriler)
                fetch_start = int(time.time() * 1000) - (2100 * iv_ms)
                klines = await fetch_all_klines(symbol, interval, fetch_start)

                if klines:
                    rows = compute_signals(klines)
                    added = await upsert_candles(rows, symbol, interval)
                    stats = await get_candle_stats(symbol, interval)
                    _status[key]["total_rows"] = stats["rows"]
                    _status[key]["last_update"] = _format_ts(int(time.time()))
                    _status[key]["error"] = None

                    await log.ainfo(
                        "data_collector_update",
                        symbol=symbol,
                        total=stats["rows"],
                    )

            except Exception as e:
                _status[key]["error"] = str(e)
                await log.aerror(
                    "data_collector_update_error",
                    symbol=symbol,
                    error=str(e),
                )
                await asyncio.sleep(30)  # Hata durumunda 30sn bekle

    except asyncio.CancelledError:
        _status[key]["running"] = False
        await log.ainfo("data_collector_stopped", symbol=symbol, interval=interval)
    except Exception as e:
        _status[key]["running"] = False
        _status[key]["error"] = str(e)
        await log.aerror(
            "data_collector_crashed", symbol=symbol, error=str(e)
        )


def start_collection(symbol: str, interval: str) -> dict:
    """Veri toplama başlat."""
    key = f"{symbol}_{interval}"
    if key in _tasks and not _tasks[key].done():
        return {"status": "already_running", "key": key}

    task = asyncio.create_task(_collection_loop(symbol, interval))
    _tasks[key] = task
    return {"status": "started", "key": key}


def stop_collection(symbol: str, interval: str) -> dict:
    """Veri toplamayı durdur."""
    key = f"{symbol}_{interval}"
    if key not in _tasks or _tasks[key].done():
        return {"status": "not_running", "key": key}

    _tasks[key].cancel()
    del _tasks[key]
    if key in _status:
        _status[key]["running"] = False
    return {"status": "stopped", "key": key}


def get_all_status() -> dict:
    """Tüm koleksiyon durumlarını döndür."""
    for key in list(_tasks.keys()):
        if _tasks[key].done():
            if key in _status:
                _status[key]["running"] = False
    return dict(_status)


def start_default_collections() -> list[dict]:
    """Config'deki varsayılan sembollerde veri toplamayı başlat."""
    from app.config import settings

    symbols_str = settings.collector_symbols.strip()
    if not symbols_str:
        return []

    interval = settings.collector_interval.strip() or "5m"
    results = []
    for sym in symbols_str.split(","):
        sym = sym.strip().upper()
        if sym:
            result = start_collection(sym, interval)
            results.append(result)
            log.info("auto_start_collection", symbol=sym, interval=interval, status=result["status"])
    return results


async def stop_all_collections():
    """Tüm koleksiyonları durdur (shutdown için)."""
    for key, task in list(_tasks.items()):
        task.cancel()
    for key, task in list(_tasks.items()):
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
    _tasks.clear()
