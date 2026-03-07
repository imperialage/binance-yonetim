"""Backtest engine – Adaptive Supertrend sinyalleri üzerinden geçmiş performans analizi.

Fonksiyonlar:
- get_all_futures_symbols(): Binance USDT-M perpetual sembol listesi (1h cache)
- fetch_klines_range(): Tarih aralığı için paginated kline çekme
- run_backtest(): Supertrend sinyallerinden trade simülasyonu
- find_optimal_sl_tp(): Grid search ile en iyi SL/TP kombinasyonu
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from app.modules.supertrend import calculate_adaptive_supertrend
from app.utils.logging import get_logger

log = get_logger(__name__)

# ── Constants ──
BINANCE_FAPI_BASE = "https://fapi.binance.com"
COMMISSION_RATE = 0.0004  # %0.04 per side
NOTIONAL = 1000  # $1000 per trade

# ── Symbols cache ──
_symbols_cache: tuple[float, list[str]] | None = None
_SYMBOLS_TTL = 3600  # 1 hour

# ── Interval → milliseconds mapping ──
INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


async def get_all_futures_symbols() -> list[str]:
    """Binance Futures USDT-M perpetual sembol listesi (TRADING durumunda)."""
    global _symbols_cache

    if _symbols_cache is not None:
        cached_at, symbols = _symbols_cache
        if time.time() - cached_at < _SYMBOLS_TTL:
            return symbols

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo")
        resp.raise_for_status()
        data = resp.json()

    symbols = sorted(
        s["symbol"]
        for s in data.get("symbols", [])
        if s.get("status") == "TRADING"
        and s.get("contractType") == "PERPETUAL"
        and s["symbol"].endswith("USDT")
    )

    _symbols_cache = (time.time(), symbols)
    return symbols


async def fetch_klines_range(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    extra_warmup: int = 110,
) -> list[list[Any]]:
    """Tarih aralığı için paginated kline çekme (Binance max 1000/istek).

    ATR warmup için `extra_warmup` kadar ekstra mum baştan çekilir.
    """
    iv_ms = INTERVAL_MS.get(interval)
    if iv_ms is None:
        raise ValueError(f"Geçersiz interval: {interval}")

    # Warmup başlangıcını geriye çek
    warmup_start_ms = start_ms - (extra_warmup * iv_ms)

    all_klines: list[list[Any]] = []
    current_start = warmup_start_ms

    async with httpx.AsyncClient(timeout=15) as client:
        while current_start < end_ms:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": current_start,
                "endTime": end_ms,
                "limit": 1000,
            }
            resp = await client.get(
                f"{BINANCE_FAPI_BASE}/fapi/v1/klines", params=params
            )
            resp.raise_for_status()
            batch = resp.json()

            if not batch:
                break

            all_klines.extend(batch)

            # Sonraki batch başlangıcı: son mumun açılış zamanı + 1 interval
            last_open_time = int(batch[-1][0])
            current_start = last_open_time + iv_ms

            if len(batch) < 1000:
                break

            # Rate limit koruması
            await asyncio.sleep(0.1)

    # Duplicate temizleme (overlap olabilir)
    seen: set[int] = set()
    unique: list[list[Any]] = []
    for k in all_klines:
        t = int(k[0])
        if t not in seen:
            seen.add(t)
            unique.append(k)

    unique.sort(key=lambda k: int(k[0]))
    return unique


def run_backtest(
    klines: list[list[Any]],
    tp_pct: float,
    sl_pct: float,
    start_ms: int,
    supertrend_data: list[dict] | None = None,
    candles_data: list[dict] | None = None,
) -> dict:
    """Supertrend direction değişimlerinden sinyal üretip backtest çalıştır.

    Args:
        klines: Ham kline verisi (warmup dahil)
        tp_pct: Take profit yüzdesi (ör: 1.5 = %1.5)
        sl_pct: Stop loss yüzdesi (ör: 1.0 = %1.0)
        start_ms: Backtest başlangıç zamanı (warmup hariç)
        supertrend_data: Önceden hesaplanmış supertrend (optimize için)
        candles_data: Önceden hesaplanmış candle verisi (optimize için)

    Returns:
        {summary, trades, daily_summary}
    """
    # Supertrend hesapla (verilmediyse)
    if supertrend_data is None or candles_data is None:
        candles_data, supertrend_data = calculate_adaptive_supertrend(klines)

    if len(supertrend_data) < 2:
        return _empty_result()

    start_sec = start_ms // 1000
    tp_mult = tp_pct / 100.0
    sl_mult = sl_pct / 100.0
    commission = COMMISSION_RATE * 2  # Giriş + çıkış

    trades: list[dict] = []
    position: dict | None = None  # {side, entry_price, entry_time, entry_idx}

    for i in range(1, len(supertrend_data)):
        t = supertrend_data[i]["time"]
        prev_dir = supertrend_data[i - 1]["direction"]
        curr_dir = supertrend_data[i]["direction"]

        candle = candles_data[i]
        high = candle["high"]
        low = candle["low"]
        close = candle["close"]

        # Açık pozisyon varsa SL/TP kontrol et
        if position is not None:
            entry = position["entry_price"]
            side = position["side"]

            if side == "LONG":
                sl_price = entry * (1 - sl_mult)
                tp_price = entry * (1 + tp_mult)
                hit_sl = low <= sl_price
                hit_tp = high >= tp_price
            else:  # SHORT
                sl_price = entry * (1 + sl_mult)
                tp_price = entry * (1 - tp_mult)
                hit_sl = high >= sl_price
                hit_tp = low <= tp_price

            # SL öncelikli (worst-case)
            if hit_sl:
                exit_price = sl_price
                pnl_pct = -sl_pct - (commission * 100)
                trades.append(_make_trade(position, t, exit_price, pnl_pct, "SL"))
                position = None
            elif hit_tp:
                exit_price = tp_price
                pnl_pct = tp_pct - (commission * 100)
                trades.append(_make_trade(position, t, exit_price, pnl_pct, "TP"))
                position = None
            elif prev_dir != curr_dir:
                # Ters sinyal geldi - pozisyonu kapat
                exit_price = close
                if side == "LONG":
                    raw_pnl = ((exit_price - entry) / entry) * 100
                else:
                    raw_pnl = ((entry - exit_price) / entry) * 100
                pnl_pct = raw_pnl - (commission * 100)
                trades.append(
                    _make_trade(position, t, exit_price, pnl_pct, "SIGNAL")
                )
                position = None

        # Sadece backtest aralığındaki sinyalleri işle
        if t < start_sec:
            continue

        # Yeni sinyal: direction değişimi
        if prev_dir != curr_dir and position is None:
            if curr_dir == -1:  # Bullish
                position = {
                    "side": "LONG",
                    "entry_price": close,
                    "entry_time": t,
                    "entry_idx": i,
                }
            else:  # Bearish
                position = {
                    "side": "SHORT",
                    "entry_price": close,
                    "entry_time": t,
                    "entry_idx": i,
                }

    # Açık kalan pozisyonu son mumda kapat
    if position is not None:
        last = candles_data[-1]
        exit_price = last["close"]
        entry = position["entry_price"]
        side = position["side"]
        if side == "LONG":
            raw_pnl = ((exit_price - entry) / entry) * 100
        else:
            raw_pnl = ((entry - exit_price) / entry) * 100
        pnl_pct = raw_pnl - (commission * 100)
        trades.append(
            _make_trade(position, last["time"], exit_price, pnl_pct, "OPEN")
        )

    return _build_result(trades)


def find_optimal_sl_tp(
    klines: list[list[Any]],
    start_ms: int,
) -> dict:
    """Grid search ile en iyi SL/TP kombinasyonunu bul.

    SL: %0.2 - %3.0 (15 adım)
    TP: %0.2 - %3.0 (15 adım)
    Toplam: 225 kombinasyon
    """
    # Supertrend'i 1 kez hesapla
    candles_data, supertrend_data = calculate_adaptive_supertrend(klines)

    sl_values = [0.2 + i * 0.2 for i in range(15)]  # 0.2, 0.4, ... 3.0
    tp_values = [0.2 + i * 0.2 for i in range(15)]

    results: list[dict] = []

    for sl in sl_values:
        for tp in tp_values:
            result = run_backtest(
                klines, tp, sl, start_ms,
                supertrend_data=supertrend_data,
                candles_data=candles_data,
            )
            summary = result["summary"]
            total_trades = summary["total_trades"]
            if total_trades < 3:
                continue

            pnl = summary["total_pnl_pct"]
            win_rate = summary["win_rate"]
            drawdown = summary["max_drawdown_pct"]

            # Skor: PnL×0.5 + win_rate×0.3 - drawdown×0.2
            score = pnl * 0.5 + win_rate * 0.3 - drawdown * 0.2

            results.append({
                "sl_pct": round(sl, 1),
                "tp_pct": round(tp, 1),
                "total_pnl_pct": round(pnl, 2),
                "win_rate": round(win_rate, 1),
                "total_trades": total_trades,
                "max_drawdown_pct": round(drawdown, 2),
                "profit_factor": summary["profit_factor"],
                "score": round(score, 2),
            })

    results.sort(key=lambda x: x["score"], reverse=True)
    top5 = results[:5]
    best = top5[0] if top5 else None

    return {
        "best": best,
        "top5": top5,
        "total_combinations": len(results),
    }


# ── Helpers ──


def _make_trade(
    position: dict, exit_time: int, exit_price: float, pnl_pct: float, reason: str
) -> dict:
    pnl_dollar = NOTIONAL * (pnl_pct / 100)
    return {
        "side": position["side"],
        "entry_time": position["entry_time"],
        "entry_price": round(position["entry_price"], 6),
        "exit_time": exit_time,
        "exit_price": round(exit_price, 6),
        "pnl_pct": round(pnl_pct, 4),
        "pnl_dollar": round(pnl_dollar, 2),
        "reason": reason,
    }


def _build_result(trades: list[dict]) -> dict:
    if not trades:
        return _empty_result()

    total_trades = len(trades)
    wins = [t for t in trades if t["pnl_pct"] > 0]
    losses = [t for t in trades if t["pnl_pct"] <= 0]
    win_count = len(wins)
    win_rate = (win_count / total_trades) * 100 if total_trades > 0 else 0

    total_pnl_pct = sum(t["pnl_pct"] for t in trades)
    total_pnl_dollar = sum(t["pnl_dollar"] for t in trades)

    avg_win = (
        sum(t["pnl_pct"] for t in wins) / len(wins) if wins else 0
    )
    avg_loss = (
        sum(t["pnl_pct"] for t in losses) / len(losses) if losses else 0
    )

    # Max drawdown (kümülatif PnL üzerinden)
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    cumulative_series: list[float] = []
    for t in trades:
        cumulative += t["pnl_pct"]
        cumulative_series.append(cumulative)
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    # Kümülatif PnL trade'lere ekle
    cum = 0.0
    for t in trades:
        cum += t["pnl_pct"]
        t["cumulative_pnl_pct"] = round(cum, 4)

    # Profit factor
    gross_profit = sum(t["pnl_pct"] for t in wins)
    gross_loss = abs(sum(t["pnl_pct"] for t in losses))
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else 999.99

    # Günlük özet
    daily: dict[str, dict] = {}
    for t in trades:
        # Unix timestamp → YYYY-MM-DD
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(t["entry_time"], tz=timezone.utc)
        day_key = dt.strftime("%Y-%m-%d")
        if day_key not in daily:
            daily[day_key] = {"date": day_key, "trades": 0, "pnl_pct": 0.0}
        daily[day_key]["trades"] += 1
        daily[day_key]["pnl_pct"] += t["pnl_pct"]

    daily_summary = sorted(daily.values(), key=lambda x: x["date"])
    for d in daily_summary:
        d["pnl_pct"] = round(d["pnl_pct"], 4)

    summary = {
        "total_trades": total_trades,
        "win_count": win_count,
        "loss_count": total_trades - win_count,
        "win_rate": round(win_rate, 1),
        "total_pnl_pct": round(total_pnl_pct, 4),
        "total_pnl_dollar": round(total_pnl_dollar, 2),
        "avg_win_pct": round(avg_win, 4),
        "avg_loss_pct": round(avg_loss, 4),
        "max_drawdown_pct": round(max_dd, 4),
        "profit_factor": profit_factor,
    }

    return {
        "summary": summary,
        "trades": trades,
        "daily_summary": daily_summary,
    }


def _empty_result() -> dict:
    return {
        "summary": {
            "total_trades": 0,
            "win_count": 0,
            "loss_count": 0,
            "win_rate": 0,
            "total_pnl_pct": 0,
            "total_pnl_dollar": 0,
            "avg_win_pct": 0,
            "avg_loss_pct": 0,
            "max_drawdown_pct": 0,
            "profit_factor": 0,
        },
        "trades": [],
        "daily_summary": [],
    }
