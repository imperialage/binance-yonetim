"""NW Color Flip — HA mumlari uzerinde Nadaraya-Watson kernel simulasyonu.

Pine Script nw_color_flip.pine'in birebir Python portu.
Heikin-Ashi mumlari hesaplar, NW kernel uygular, renk degisiminde islem acar.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
from fastapi import APIRouter

from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()

_TZ_IST = timezone(timedelta(hours=3))

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}


def _fmt_ist(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=_TZ_IST).strftime("%Y-%m-%d %H:%M")


def _convert_to_ha(klines: list) -> list[dict]:
    """Normal klines → Heikin-Ashi."""
    if not klines:
        return []
    result = []
    prev_ha_open = float(klines[0][1])
    prev_ha_close = (float(klines[0][1]) + float(klines[0][2]) + float(klines[0][3]) + float(klines[0][4])) / 4
    for k in klines:
        o, h, l, c = float(k[1]), float(k[2]), float(k[3]), float(k[4])
        ha_close = (o + h + l + c) / 4
        ha_open = (prev_ha_open + prev_ha_close) / 2
        ha_high = max(h, ha_open, ha_close)
        ha_low = min(l, ha_open, ha_close)
        result.append({
            "time": int(k[0]) // 1000,
            "open": ha_open, "high": ha_high, "low": ha_low, "close": ha_close,
            "real_open": o, "real_high": h, "real_low": l, "real_close": c,
            "volume": float(k[5]),
        })
        prev_ha_open = ha_open
        prev_ha_close = ha_close
    return result


def _rq_kernel(closes: list[float], idx: int, h: int, alpha: float) -> float:
    """Rational Quadratic Kernel — Pine Script rqKernel fonksiyonunun portu."""
    sw = 0.0
    sv = 0.0
    for j in range(min(h, idx + 1)):
        w = (1.0 + (j * j) / (2.0 * alpha * h * h)) ** (-alpha)
        sw += w
        val = closes[idx - j] if idx - j >= 0 else closes[0]
        sv += w * val
    return sv / sw if sw > 0 else closes[idx]


def simulate_nw_color_flip(
    ha_candles: list[dict],
    h: int = 8,
    alpha: float = 8.0,
    x_0: int = 25,
    comm_pct: float = 0.0008,
) -> dict[str, Any]:
    """NW Color Flip simulasyonu — HA mumlari uzerinde."""
    n = len(ha_candles)
    if n < x_0 + 2:
        return {"candles": [], "signals": [], "trades": [], "stats": {}}

    # TradingView HA chart'ta close = HA close
    # Pine Script'te rqKernel(close) → HA close kullanir
    # Entry fiyati da HA close (simulasyon TradingView ile birebir)
    closes = [c["close"] for c in ha_candles]

    # NW kernel hesapla
    yhat = [0.0] * n
    for i in range(n):
        if i >= x_0:
            yhat[i] = _rq_kernel(closes, i, h, alpha)
        else:
            yhat[i] = closes[i]

    # Trend ve renk degisimi
    is_up = [False] * n
    color_changed = [False] * n
    for i in range(1, n):
        is_up[i] = yhat[i] > yhat[i - 1]
        if i > 1:
            color_changed[i] = is_up[i] != is_up[i - 1]

    # State machine — Pine Script ile birebir
    state = 0  # 0=flat, 1=long, -1=short
    entry = 0.0
    entry_bar = 0
    entry_time = 0

    signals: list[dict] = []
    trades: list[dict] = []

    for i in range(n):
        if not color_changed[i]:
            continue

        # Onceki pozisyonu kapat
        if state == 1:
            pct = (closes[i] - entry) / entry * 100 - comm_pct * 100
            trades.append({
                "entry_idx": entry_bar, "exit_idx": i,
                "entry_time": entry_time, "exit_time": ha_candles[i]["time"],
                "entry_date": _fmt_ist(entry_time), "exit_date": _fmt_ist(ha_candles[i]["time"]),
                "direction": "BUY", "entry_price": entry, "exit_price": closes[i],
                "reason": "COLOR_FLIP", "pnl_pct": round(pct, 4),
            })
            state = 0
        elif state == -1:
            pct = (entry - closes[i]) / entry * 100 - comm_pct * 100
            trades.append({
                "entry_idx": entry_bar, "exit_idx": i,
                "entry_time": entry_time, "exit_time": ha_candles[i]["time"],
                "entry_date": _fmt_ist(entry_time), "exit_date": _fmt_ist(ha_candles[i]["time"]),
                "direction": "SELL", "entry_price": entry, "exit_price": closes[i],
                "reason": "COLOR_FLIP", "pnl_pct": round(pct, 4),
            })
            state = 0

        # Yeni pozisyon
        entry = closes[i]
        entry_bar = i
        entry_time = ha_candles[i]["time"]
        if is_up[i]:
            state = 1
            signals.append({
                "idx": i, "time": ha_candles[i]["time"],
                "date": _fmt_ist(ha_candles[i]["time"]),
                "direction": "BUY", "entry_price": entry,
                "nw_value": round(yhat[i], 6),
                "nw_prev": round(yhat[i - 1], 6),
            })
        else:
            state = -1
            signals.append({
                "idx": i, "time": ha_candles[i]["time"],
                "date": _fmt_ist(ha_candles[i]["time"]),
                "direction": "SELL", "entry_price": entry,
                "nw_value": round(yhat[i], 6),
                "nw_prev": round(yhat[i - 1], 6),
            })

    # Candle data + yhat
    candle_data = []
    for i in range(n):
        c = ha_candles[i]
        candle_data.append({
            "idx": i, "time": c["time"],
            "open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"],
            "real_close": c["real_close"], "volume": c["volume"],
            "nw": round(yhat[i], 6) if i >= x_0 else None,
            "trend": "UP" if is_up[i] else "DOWN",
            "flip": color_changed[i],
        })

    # Stats
    n_trades = len(trades)
    n_win = sum(1 for t in trades if t["pnl_pct"] > 0)
    n_loss = sum(1 for t in trades if t["pnl_pct"] <= 0)
    sum_pnl = sum(t["pnl_pct"] for t in trades)
    sum_win = sum(t["pnl_pct"] for t in trades if t["pnl_pct"] > 0)
    sum_loss = sum(t["pnl_pct"] for t in trades if t["pnl_pct"] <= 0)
    wr = (n_win / n_trades * 100) if n_trades > 0 else 0
    pf = (sum_win / abs(sum_loss)) if sum_loss < 0 else 0
    avg_win = sum_win / n_win if n_win > 0 else 0
    avg_loss = sum_loss / n_loss if n_loss > 0 else 0

    stats = {
        "n_signals": len(signals),
        "n_trades": n_trades,
        "n_win": n_win,
        "n_loss": n_loss,
        "win_rate": round(wr, 2),
        "sum_pnl_pct": round(sum_pnl, 2),
        "profit_factor": round(pf, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "open_position": state != 0,
        "open_side": "LONG" if state == 1 else ("SHORT" if state == -1 else None),
        "open_entry": round(entry, 6) if state != 0 else None,
    }

    return {"candles": candle_data, "signals": signals, "trades": trades, "stats": stats}


@router.get("/api/nw-sim")
async def nw_sim(
    symbol: str = "MYXUSDT",
    interval: str = "5m",
    h: int = 8,
    alpha: float = 8.0,
    x_0: int = 25,
    comm_pct: float = 0.08,
    bars: int = 500,
) -> dict[str, Any]:
    """NW Color Flip — Binance klines → HA donusum → kernel simulasyon."""
    sym = symbol.upper()
    bars = max(50, min(bars, 1500))

    end_ms = int(time.time() * 1000)
    start_ms = end_ms - (bars + x_0 + 50) * INTERVAL_MS.get(interval, 300_000)

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get("https://fapi.binance.com/fapi/v1/klines", params={
                "symbol": sym, "interval": interval,
                "startTime": start_ms, "endTime": end_ms, "limit": 1500,
            })
            resp.raise_for_status()
            klines = resp.json()
    except Exception as e:
        return {"error": str(e), "candles": [], "signals": [], "trades": [], "stats": {}}

    if not klines:
        return {"candles": [], "signals": [], "trades": [], "stats": {}}

    ha_candles = _convert_to_ha(klines)

    result = simulate_nw_color_flip(
        ha_candles=ha_candles,
        h=h,
        alpha=alpha,
        x_0=x_0,
        comm_pct=comm_pct / 100.0,
    )

    result["symbol"] = sym
    result["interval"] = interval
    result["params"] = {"h": h, "alpha": alpha, "x_0": x_0, "comm_pct": comm_pct, "bars": bars}
    return result
