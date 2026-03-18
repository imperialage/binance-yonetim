"""Binance mum verisi toplama + Adaptive SuperTrend sinyal hesaplama modülü.

Sürekli çalışan background task ile yeni mumları çeker, SuperTrend hesaplar,
sinyalleri tespit eder ve CSV'ye kaydeder.
"""

from __future__ import annotations

import asyncio
import csv
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from app.modules.supertrend import calculate_adaptive_supertrend
from app.utils.logging import get_logger

log = get_logger(__name__)

BINANCE_FAPI_BASE = "https://fapi.binance.com"
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

# CSV sütun başlıkları
CSV_HEADERS = [
    "date", "open", "high", "low", "close", "volume",
    "supertrend", "direction", "cluster", "signal",
]

# Interval → milisaniye
INTERVAL_MS: dict[str, int] = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000,
    "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000,
    "4h": 14_400_000, "1d": 86_400_000,
}

# Aktif koleksiyon görevleri
_tasks: dict[str, asyncio.Task] = {}
_status: dict[str, dict] = {}


def _csv_path(symbol: str, interval: str) -> Path:
    """CSV dosya yolunu döndür."""
    return DATA_DIR / f"{symbol}_{interval}_supertrend.csv"


def _format_ts(unix_sec: int) -> str:
    """Unix timestamp → ISO 8601 string."""
    return datetime.fromtimestamp(unix_sec, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


async def _fetch_klines(
    symbol: str,
    interval: str,
    start_ms: int | None = None,
    end_ms: int | None = None,
    limit: int = 1000,
) -> list[list[Any]]:
    """Binance'dan kline verisi çek."""
    params: dict[str, Any] = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }
    if start_ms is not None:
        params["startTime"] = start_ms
    if end_ms is not None:
        params["endTime"] = end_ms

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{BINANCE_FAPI_BASE}/fapi/v1/klines", params=params)
        resp.raise_for_status()
        return resp.json()


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
    """Kline verisinden SuperTrend + sinyal hesapla, CSV satırları döndür."""
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


def save_to_csv(rows: list[dict], filepath: Path) -> int:
    """Satırları CSV dosyasına yaz (üzerine yazar)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def append_to_csv(rows: list[dict], filepath: Path) -> int:
    """Yeni satırları CSV'ye ekle (varsa mevcut tarihleri atla)."""
    existing_dates: set[str] = set()
    if filepath.exists():
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_dates.add(row["date"])

    new_rows = [r for r in rows if r["date"] not in existing_dates]
    if not new_rows:
        return 0

    write_header = not filepath.exists() or os.path.getsize(filepath) == 0
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)
    return len(new_rows)


def read_csv_tail(filepath: Path, limit: int = 100) -> list[dict]:
    """CSV'nin son N satırını oku."""
    if not filepath.exists():
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    return rows[-limit:]


def get_csv_stats(filepath: Path) -> dict:
    """CSV dosyası hakkında istatistikler."""
    if not filepath.exists():
        return {"exists": False, "rows": 0, "size_kb": 0}

    size_kb = round(os.path.getsize(filepath) / 1024, 1)
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return {"exists": True, "rows": 0, "size_kb": size_kb}

    signals = [r for r in rows if r.get("signal")]
    buy_count = sum(1 for r in rows if r.get("signal") == "BUY")
    sell_count = sum(1 for r in rows if r.get("signal") == "SELL")

    return {
        "exists": True,
        "rows": len(rows),
        "size_kb": size_kb,
        "first_date": rows[0]["date"],
        "last_date": rows[-1]["date"],
        "total_signals": len(signals),
        "buy_signals": buy_count,
        "sell_signals": sell_count,
    }


# ── Background collection task ──


async def _collection_loop(symbol: str, interval: str):
    """Sürekli çalışan veri toplama döngüsü."""
    iv_ms = INTERVAL_MS[interval]
    filepath = _csv_path(symbol, interval)
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
        # İlk çalıştırmada: varsa CSV'deki son tarihi bul, yoksa 30 gün geriden başla
        last_ms: int | None = None
        if filepath.exists():
            stats = get_csv_stats(filepath)
            if stats["rows"] > 0:
                last_date = stats["last_date"]
                dt = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc
                )
                last_ms = int(dt.timestamp() * 1000)

        if last_ms is None:
            # 30 gün geriden başla (warmup için yeterli)
            last_ms = int(time.time() * 1000) - (30 * 86400 * 1000)

        # İlk tam çekme (warmup dahil)
        warmup_start = last_ms - (2000 * iv_ms)  # 2000 mum warmup
        klines = await fetch_all_klines(symbol, interval, warmup_start)

        if klines:
            rows = compute_signals(klines)
            # İlk seferde: warmup dışı satırları kaydet
            start_sec = last_ms // 1000
            # Eğer ilk çalıştırmaysa tümünü kaydet, devamsa sadece yenileri
            if not filepath.exists() or os.path.getsize(filepath) == 0:
                save_to_csv(rows, filepath)
            else:
                new_rows = [r for r in rows if r["date"] > stats["last_date"]]
                if new_rows:
                    # Sinyalleri doğru hesaplamak için son birkaç mumu da dahil et
                    append_to_csv(new_rows, filepath)

            _status[key]["total_rows"] = get_csv_stats(filepath)["rows"]
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
                    added = append_to_csv(rows, filepath)
                    stats = get_csv_stats(filepath)
                    _status[key]["total_rows"] = stats["rows"]
                    _status[key]["last_update"] = _format_ts(int(time.time()))
                    _status[key]["error"] = None

                    if added > 0:
                        await log.ainfo(
                            "data_collector_update",
                            symbol=symbol,
                            new_rows=added,
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
    # Tamamlanmış task'ları temizle
    for key in list(_tasks.keys()):
        if _tasks[key].done():
            if key in _status:
                _status[key]["running"] = False

    return dict(_status)


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
