"""Binance mum verisi toplama + Adaptive SuperTrend sinyal hesaplama modülü.

Sürekli çalışan background task ile yeni mumları çeker, SuperTrend hesaplar,
sinyalleri tespit eder ve SQLite'a kaydeder.

Otomatik sinyal tespiti: Yeni mum kapandığında SuperTrend yön değişimi varsa
filtrelerden geçirir ve işlem tetikler (TradingView'a gerek yok).
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

# Son işlenen sinyal zamanı (duplicate önleme)
_last_signal_time: dict[str, int] = {}

# Initial fetch kilidi — bir seferde sadece 1 sembol initial fetch yapar
_initial_fetch_lock = asyncio.Lock()


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

    # Proxy ayarını binance_client ile aynı şekilde al
    from app.config import settings as _settings
    proxy_url = _settings.binance_proxy_url or None

    async with httpx.AsyncClient(timeout=15, proxy=proxy_url) as client:
        while current_start < end_ms:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": current_start,
                "endTime": end_ms,
                "limit": 1500,
            }

            # Rate limit / IP ban retry (429, 418)
            for attempt in range(5):
                resp = await client.get(
                    f"{BINANCE_FAPI_BASE}/fapi/v1/klines", params=params
                )
                if resp.status_code in (429, 418):
                    wait = min(2 ** attempt * 2, 30)  # 2, 4, 8, 16, 30s
                    await log.awarning(
                        "binance_rate_limit",
                        symbol=symbol,
                        status=resp.status_code,
                        attempt=attempt + 1,
                        wait=wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                break
            else:
                raise httpx.HTTPStatusError(
                    f"Binance {resp.status_code} after 5 retries",
                    request=resp.request,
                    response=resp,
                )

            batch = resp.json()

            if not batch:
                break

            all_klines.extend(batch)
            last_open_time = int(batch[-1][0])
            current_start = last_open_time + iv_ms

            if len(batch) < 1500:
                break

            await asyncio.sleep(0.3)  # Rate limit koruması

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


# ── Auto-signal detection & trade dispatch ──


async def _process_auto_signal(symbol: str, signal_row: dict, interval: str) -> None:
    """Yeni SuperTrend sinyali tespit edildi — filtrele, logla, işlem tetikle.

    Bu fonksiyon sayesinde TradingView webhook'a gerek kalmaz.
    data_collector her mum kapanışında sinyali otomatik tespit eder.
    """
    from app.config import settings
    from app.modules.st_filter_engine import run_filters
    from app.modules.st_entry_optimizer import check_entry
    from app.modules.st_signal_logger import log_st_signal
    from app.modules.trade_executor import execute_trade

    direction = signal_row["signal"]  # "BUY" or "SELL"
    band = signal_row["cluster"]       # "HIGH", "MID", "LOW"
    price = signal_row["close"]
    open_time_ms = signal_row["open_time"]

    iv_ms = INTERVAL_MS.get(interval, 300_000)
    # Sinyal zamanı = mum kapanış zamanı
    signal_ts = (open_time_ms + iv_ms) // 1000
    signal_dt = datetime.fromtimestamp(signal_ts, tz=timezone.utc)
    # Log ve DB'ye UTC+3 (Istanbul) olarak kaydet — mum tarihleriyle tutarlı
    dt_str = datetime.fromtimestamp(signal_ts, tz=_TZ_ISTANBUL).strftime("%Y-%m-%d %H:%M:%S")

    await log.ainfo(
        "auto_signal_detected",
        symbol=symbol,
        direction=direction,
        band=band,
        price=price,
        time=dt_str,
    )

    # ── 1. Filtreler (saat, band, hacim, funding, istatistik) ──
    passed, filter_results, vol_ratio = await run_filters(
        symbol=symbol,
        direction=direction,
        band=band,
        price=price,
        signal_time=signal_dt,
    )

    if not passed:
        failed = next((f for f in filter_results if not f["pass"]), None)
        skip_filter = failed["name"] if failed else "unknown"
        skip_reason = failed["reason"] if failed else "unknown"

        await log.ainfo(
            "auto_signal_filtered",
            symbol=symbol,
            direction=direction,
            filter=skip_filter,
            reason=skip_reason,
        )

        await log_st_signal(
            dt=dt_str,
            symbol=symbol,
            direction=direction,
            band=band,
            price=price,
            vol_ratio=vol_ratio,
            entered=False,
            skip_filter=skip_filter,
            skip_reason=skip_reason,
        )
        return

    # ── 2. Entry optimization (momentum, body ratio) ──
    entry_result = await check_entry(
        symbol=symbol,
        direction=direction,
        price=price,
        signal_time=signal_ts,
    )

    if not entry_result.passed:
        await log.ainfo(
            "auto_signal_entry_rejected",
            symbol=symbol,
            direction=direction,
            reason=entry_result.reason,
        )

        await log_st_signal(
            dt=dt_str,
            symbol=symbol,
            direction=direction,
            band=band,
            price=price,
            vol_ratio=vol_ratio,
            entered=False,
            skip_filter="entry_optimizer",
            skip_reason=entry_result.reason,
        )
        return

    # ── 3. Sinyal geçti — logla ──
    row_id = await log_st_signal(
        dt=dt_str,
        symbol=symbol,
        direction=direction,
        band=band,
        price=price,
        vol_ratio=vol_ratio,
        entered=True,
    )

    await log.ainfo(
        "auto_signal_accepted",
        symbol=symbol,
        direction=direction,
        band=band,
        price=price,
        signal_id=row_id,
        trading_enabled=settings.trading_enabled,
    )

    # ── 4. İşlem tetikle (trading_enabled ise) ──
    if settings.trading_enabled:
        from app.config import get_symbol_config
        sym_cfg = get_symbol_config(symbol)

        event_id = f"auto-{row_id}-{signal_ts}"
        tf = settings.trading_timeframes.split(",")[0].strip() or "5m"
        asyncio.create_task(execute_trade(
            symbol=symbol,
            signal=direction,
            price=price,
            event_id=event_id,
            tf=tf,
            tp_pct=sym_cfg.get("tp_pct"),
            sl_pct=sym_cfg.get("sl_pct"),
        ))
        await log.ainfo("auto_trade_dispatched", symbol=symbol, direction=direction, price=price)
    else:
        await log.ainfo("auto_trade_skipped_disabled", symbol=symbol, direction=direction)


def _find_latest_signal(rows: list[dict]) -> dict | None:
    """Hesaplanan satırlardan en son sinyali bul (sondan başa)."""
    for row in reversed(rows):
        if row.get("signal") in ("BUY", "SELL"):
            return row
    return None


# ── Initial fetch (sıralı — rate limit koruması) ──


async def _do_initial_fetch(
    symbol: str, interval: str, iv_ms: int, key: str
) -> None:
    """DB'deki en son mumdan itibaren veri çek. Lock altında çağrılır."""
    last_ms = await get_last_open_time(symbol, interval)

    if last_ms is None:
        # DB boş → 2 yıl geriden başla
        last_ms = int(time.time() * 1000) - (730 * 86400 * 1000)

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
        count = await upsert_candles(rows, symbol, interval)
        stats = await get_candle_stats(symbol, interval)
        _status[key]["total_rows"] = stats["rows"]
        _status[key]["last_update"] = _format_ts(int(time.time()))

    await log.ainfo(
        "data_collector_initial_fetch",
        symbol=symbol,
        rows=_status[key]["total_rows"],
    )


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
        # Sıralı başlatma kilidi — aynı anda birden fazla sembol
        # initial fetch yapmasın (rate limit koruması)
        await _initial_fetch_lock.acquire()
        try:
            await _do_initial_fetch(symbol, interval, iv_ms, key)
        finally:
            _initial_fetch_lock.release()

        # Sürekli güncelleme döngüsü
        while True:
            # Bir sonraki mum kapanışını bekle
            now_ms = int(time.time() * 1000)
            current_candle_start = (now_ms // iv_ms) * iv_ms
            next_candle_close = current_candle_start + iv_ms
            wait_sec = max(5, (next_candle_close - now_ms) / 1000 + 2)  # +2sn buffer

            await asyncio.sleep(wait_sec)

            try:
                # Hızlı güncelleme: Binance'tan son 10 mumu çek
                # SuperTrend hesabı için DB'deki warmup verisini kullan
                fetch_start = int(time.time() * 1000) - (10 * iv_ms)
                new_klines = await fetch_all_klines(symbol, interval, fetch_start)

                if new_klines:
                    # DB'den warmup mumlarını al + yeni mumları birleştir
                    from app.modules.candle_store import get_raw_klines
                    db_klines = await get_raw_klines(symbol, interval, limit=2100)

                    # Merge: Binance'tan gelen TAZE veri öncelik alır
                    # (DB'deki eski/tamamlanmamış mum verisi yerine)
                    new_times = set()
                    merged = []
                    for k in new_klines:
                        t = int(k[0])
                        if t not in new_times:
                            new_times.add(t)
                            merged.append(k)
                    for k in db_klines:
                        t = int(k[0])
                        if t not in new_times:
                            merged.append(k)
                    merged.sort(key=lambda k: int(k[0]))

                    rows = compute_signals(merged)
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

                    # ── Otomatik sinyal tespiti ──
                    # Şu an oluşan (kapanmamış) mumu hariç tut —
                    # sadece kapanmış mumlardan sinyal al
                    now_ms = int(time.time() * 1000)
                    current_candle_open = (now_ms // iv_ms) * iv_ms
                    closed_rows = [r for r in rows if r["open_time"] < current_candle_open]

                    latest_signal = _find_latest_signal(closed_rows)
                    if latest_signal is not None:
                        sig_time = latest_signal["open_time"]
                        prev_time = _last_signal_time.get(key, 0)

                        if sig_time > prev_time:
                            _last_signal_time[key] = sig_time
                            # Son 3 interval içinde oluşan sinyalleri işle
                            age_ms = now_ms - sig_time
                            if age_ms < iv_ms * 3:
                                try:
                                    await _process_auto_signal(symbol, latest_signal, interval)
                                except Exception as sig_err:
                                    await log.aerror(
                                        "auto_signal_processing_error",
                                        symbol=symbol,
                                        error=str(sig_err),
                                    )
                            else:
                                await log.ainfo(
                                    "auto_signal_skipped_old",
                                    symbol=symbol,
                                    direction=latest_signal["signal"],
                                    age_ms=age_ms,
                                    limit_ms=iv_ms * 3,
                                )
                        else:
                            # Zaten işlenmiş sinyal — debug log
                            if sig_time == prev_time:
                                pass  # Aynı sinyal, normal
                            else:
                                await log.awarning(
                                    "auto_signal_time_mismatch",
                                    symbol=symbol,
                                    sig_time=sig_time,
                                    prev_time=prev_time,
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
        _status[key]["error"] = str(e)
        await log.aerror(
            "data_collector_crashed", symbol=symbol, error=str(e)
        )
        # Crash recovery: 60sn bekle ve yeniden başlat
        await log.ainfo("data_collector_restarting", symbol=symbol, wait=60)
        await asyncio.sleep(60)
        _status[key]["running"] = True
        _status[key]["error"] = None
        # Yeniden başlat (recursive olmadan, yeni task olarak)
        _tasks[key] = asyncio.create_task(_collection_loop(symbol, interval))


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
    """Config'deki varsayılan sembollerde veri toplamayı başlat.

    Semboller arası 10sn gecikme ile başlatır (rate limit koruması).
    """
    from app.config import settings

    symbols_str = settings.collector_symbols.strip()
    if not symbols_str:
        return []

    interval = settings.collector_interval.strip() or "5m"
    symbols = [s.strip().upper() for s in symbols_str.split(",") if s.strip()]

    # Aşamalı başlatma: her sembol 10sn arayla
    async def _staggered_start():
        for i, sym in enumerate(symbols):
            if i > 0:
                await asyncio.sleep(10)  # Semboller arası bekleme
            start_collection(sym, interval)
            await log.ainfo("auto_start_collection", symbol=sym, interval=interval, delay=i * 10)

    asyncio.create_task(_staggered_start())

    return [{"status": "scheduled", "key": f"{s}_{interval}"} for s in symbols]


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
