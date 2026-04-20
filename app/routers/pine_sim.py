"""Pine Script Hidden RSI Divergence — birebir Python simulasyonu.

Bu router signal_engine ve Binance pozisyon yonetiminden BAGIMSIZDIR.
Sadece Binance API'den klines ceker, Pine Script'in mantigini birebir
Python'a port eder ve sonuclari gosterir.

Kaynak: indicators/hidden_rsi_div_myxusdt.pine (state machine + usedA + SL/TP).
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
from fastapi import APIRouter, Request

from app.modules.rsi_calculator import calculate_rsi, calculate_rsi_with_state
from app.modules.pine_live_store import (
    insert_live_signal,
    query_live_signals,
    upsert_params,
    get_params,
    get_all_active_params,
)
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
    "6h": 21_600_000, "12h": 43_200_000, "1d": 86_400_000,
}

_TZ_IST = timezone(timedelta(hours=3))


def _fmt_ist(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=_TZ_IST).strftime("%Y-%m-%d %H:%M")


def simulate_pine_hidden_div(
    klines: list,
    rsi_len: int,
    long_thresh: float,
    short_thresh: float,
    max_gap: int,
    entry_buffer: float,  # ondalik (0.001 = %0.1)
    tp_pct: float,        # ondalik
    sl_pct: float,        # ondalik
    comm_pct: float,      # ondalik
) -> dict[str, Any]:
    """Pine Script Hidden RSI Divergence — birebir port.

    Pine Script kaynak: indicators/hidden_rsi_div_myxusdt.pine
    Davranis:
    - Bar kapanisinda calisir (calc_on_every_tick=false)
    - SL/TP kontrolu sadece bar_index > entryBarIdx ise (giris bar'inda hayir)
    - Ayni barda kapanan pozisyon icin yeni sinyal aranmaz (didClose=true)
    - usedA: bir kez giris yapilan A mumu tekrar kullanilmaz
    - Once SHORT, sonra LONG aranir (SHORT bulunmazsa)
    """
    n = len(klines)
    if n < rsi_len + 2:
        return {"candles": [], "signals": [], "trades": [], "stats": {}}

    times = [int(k[0]) // 1000 for k in klines]
    opens = [float(k[1]) for k in klines]
    highs = [float(k[2]) for k in klines]
    lows = [float(k[3]) for k in klines]
    closes = [float(k[4]) for k in klines]
    volumes = [float(k[5]) for k in klines]

    rsi = calculate_rsi(closes, rsi_len)

    # State (Pine Script var'lari)
    state = 0           # 0=flat, 1=long, -1=short
    entry = 0.0
    tp = 0.0
    sl = 0.0
    entry_bar_idx = -1
    entry_time = 0
    used_a: set[int] = set()

    signals: list[dict] = []
    trades: list[dict] = []

    for bar_idx in range(n):
        if rsi[bar_idx] is None:
            continue

        did_close = False

        # ── SL/TP kontrol — Pine: bar_index > entryBarIdx ─
        if state == 1 and bar_idx > entry_bar_idx:
            if lows[bar_idx] <= sl:
                pct = (sl - entry) / entry * 100 - comm_pct * 100
                trades.append({
                    "entry_idx": entry_bar_idx,
                    "exit_idx": bar_idx,
                    "entry_time": entry_time,
                    "exit_time": times[bar_idx],
                    "entry_date": _fmt_ist(entry_time),
                    "exit_date": _fmt_ist(times[bar_idx]),
                    "direction": "BUY",
                    "entry_price": entry,
                    "exit_price": sl,
                    "tp": tp,
                    "sl": sl,
                    "reason": "SL",
                    "pnl_pct": round(pct, 4),
                })
                state = 0
                did_close = True
            elif highs[bar_idx] >= tp:
                pct = (tp - entry) / entry * 100 - comm_pct * 100
                trades.append({
                    "entry_idx": entry_bar_idx,
                    "exit_idx": bar_idx,
                    "entry_time": entry_time,
                    "exit_time": times[bar_idx],
                    "entry_date": _fmt_ist(entry_time),
                    "exit_date": _fmt_ist(times[bar_idx]),
                    "direction": "BUY",
                    "entry_price": entry,
                    "exit_price": tp,
                    "tp": tp,
                    "sl": sl,
                    "reason": "TP",
                    "pnl_pct": round(pct, 4),
                })
                state = 0
                did_close = True
        elif state == -1 and bar_idx > entry_bar_idx:
            if highs[bar_idx] >= sl:
                pct = (entry - sl) / entry * 100 - comm_pct * 100
                trades.append({
                    "entry_idx": entry_bar_idx,
                    "exit_idx": bar_idx,
                    "entry_time": entry_time,
                    "exit_time": times[bar_idx],
                    "entry_date": _fmt_ist(entry_time),
                    "exit_date": _fmt_ist(times[bar_idx]),
                    "direction": "SELL",
                    "entry_price": entry,
                    "exit_price": sl,
                    "tp": tp,
                    "sl": sl,
                    "reason": "SL",
                    "pnl_pct": round(pct, 4),
                })
                state = 0
                did_close = True
            elif lows[bar_idx] <= tp:
                pct = (entry - tp) / entry * 100 - comm_pct * 100
                trades.append({
                    "entry_idx": entry_bar_idx,
                    "exit_idx": bar_idx,
                    "entry_time": entry_time,
                    "exit_time": times[bar_idx],
                    "entry_date": _fmt_ist(entry_time),
                    "exit_date": _fmt_ist(times[bar_idx]),
                    "direction": "SELL",
                    "entry_price": entry,
                    "exit_price": tp,
                    "tp": tp,
                    "sl": sl,
                    "reason": "TP",
                    "pnl_pct": round(pct, 4),
                })
                state = 0
                did_close = True

        # ── Sinyal arama — Pine: state == 0 and not didClose ─
        if state != 0 or did_close:
            continue

        sig_short = False
        sig_long = False
        sig_a_idx = -1
        sig_a_rsi = 0.0
        sig_b_rsi = rsi[bar_idx]
        sig_gap = 0
        sig_price = 0.0

        # SHORT (Bearish Hidden Divergence)
        for i in range(1, max_gap + 1):
            ai = bar_idx - i
            if ai < 0 or ai in used_a:
                continue
            if rsi[ai] is None:
                continue
            if (rsi[ai] >= short_thresh
                    and highs[bar_idx] > highs[ai]
                    and rsi[bar_idx] < rsi[ai]):
                sig_short = True
                sig_a_idx = ai
                sig_a_rsi = rsi[ai]
                sig_gap = i
                sig_price = highs[bar_idx] * (1 - entry_buffer)
                break

        # LONG (Bullish Hidden Divergence) — Pine: SHORT bulunmadiysa
        if not sig_short:
            for i in range(1, max_gap + 1):
                ai = bar_idx - i
                if ai < 0 or ai in used_a:
                    continue
                if rsi[ai] is None:
                    continue
                if (rsi[ai] <= long_thresh
                        and lows[bar_idx] < lows[ai]
                        and rsi[bar_idx] > rsi[ai]):
                    sig_long = True
                    sig_a_idx = ai
                    sig_a_rsi = rsi[ai]
                    sig_gap = i
                    sig_price = lows[bar_idx] * (1 + entry_buffer)
                    break

        if sig_long:
            state = 1
            entry = sig_price
            tp = entry * (1 + tp_pct)
            sl = entry * (1 - sl_pct)
            entry_bar_idx = bar_idx
            entry_time = times[bar_idx]
            used_a.add(sig_a_idx)
            signals.append({
                "idx": bar_idx,
                "time": times[bar_idx],
                "date": _fmt_ist(times[bar_idx]),
                "direction": "BUY",
                "entry_price": entry,
                "tp": tp,
                "sl": sl,
                "rsi_a": round(sig_a_rsi, 2),
                "rsi_b": round(sig_b_rsi, 2),
                "gap": sig_gap,
                "a_idx": sig_a_idx,
                "a_time": times[sig_a_idx],
                "a_low": lows[sig_a_idx],
                "b_low": lows[bar_idx],
            })
        elif sig_short:
            state = -1
            entry = sig_price
            tp = entry * (1 - tp_pct)
            sl = entry * (1 + sl_pct)
            entry_bar_idx = bar_idx
            entry_time = times[bar_idx]
            used_a.add(sig_a_idx)
            signals.append({
                "idx": bar_idx,
                "time": times[bar_idx],
                "date": _fmt_ist(times[bar_idx]),
                "direction": "SELL",
                "entry_price": entry,
                "tp": tp,
                "sl": sl,
                "rsi_a": round(sig_a_rsi, 2),
                "rsi_b": round(sig_b_rsi, 2),
                "gap": sig_gap,
                "a_idx": sig_a_idx,
                "a_time": times[sig_a_idx],
                "a_high": highs[sig_a_idx],
                "b_high": highs[bar_idx],
            })

    candles = [
        {
            "idx": i,
            "time": times[i],
            "open": opens[i],
            "high": highs[i],
            "low": lows[i],
            "close": closes[i],
            "volume": volumes[i],
            "rsi": rsi[i],
        }
        for i in range(n)
    ]

    # Stats
    n_tp = sum(1 for t in trades if t["reason"] == "TP")
    n_sl = sum(1 for t in trades if t["reason"] == "SL")
    n_total = len(trades)
    win_rate = (n_tp / n_total * 100) if n_total > 0 else 0
    sum_pnl = sum(t["pnl_pct"] for t in trades)
    sum_win = sum(t["pnl_pct"] for t in trades if t["pnl_pct"] > 0)
    sum_loss = sum(t["pnl_pct"] for t in trades if t["pnl_pct"] <= 0)
    pf = (sum_win / abs(sum_loss)) if sum_loss < 0 else 0

    stats = {
        "n_signals": len(signals),
        "n_trades": n_total,
        "n_tp": n_tp,
        "n_sl": n_sl,
        "n_buy": sum(1 for s in signals if s["direction"] == "BUY"),
        "n_sell": sum(1 for s in signals if s["direction"] == "SELL"),
        "win_rate": round(win_rate, 2),
        "sum_pnl_pct": round(sum_pnl, 2),
        "profit_factor": round(pf, 2),
        "open_position": state != 0,
        "open_side": "LONG" if state == 1 else ("SHORT" if state == -1 else None),
    }

    return {"candles": candles, "signals": signals, "trades": trades, "stats": stats}


@router.get("/api/pine-sim")
async def pine_sim(
    symbol: str = "MYXUSDT",
    interval: str = "5m",
    rsi_len: int = 10,
    long_thresh: float = 32.0,
    short_thresh: float = 70.0,
    max_gap: int = 12,
    entry_buffer_pct: float = 0.1,   # yuzde (UI'dan)
    tp_pct: float = 1.1,              # yuzde
    sl_pct: float = 0.35,             # yuzde
    comm_pct: float = 0.08,           # yuzde
    bars: int = 500,                  # kac mum
) -> dict[str, Any]:
    """Pine Script Hidden RSI Divergence — Binance klines + birebir simulasyon.

    bars: 100-1500 arasi (Binance limit)
    """
    sym = symbol.upper()
    bars = max(50, min(bars, 1500))

    iv_ms = INTERVAL_MS.get(interval, 300_000)
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - (bars + rsi_len + 20) * iv_ms  # warmup

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://fapi.binance.com/fapi/v1/klines",
                params={
                    "symbol": sym,
                    "interval": interval,
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": 1500,
                },
            )
            resp.raise_for_status()
            klines = resp.json()
    except Exception as e:
        await log.aerror("pine_sim_klines_error", symbol=sym, error=str(e))
        return {"error": str(e), "candles": [], "signals": [], "trades": [], "stats": {}}

    if not klines:
        return {"candles": [], "signals": [], "trades": [], "stats": {}}

    result = simulate_pine_hidden_div(
        klines=klines,
        rsi_len=rsi_len,
        long_thresh=long_thresh,
        short_thresh=short_thresh,
        max_gap=max_gap,
        entry_buffer=entry_buffer_pct / 100.0,
        tp_pct=tp_pct / 100.0,
        sl_pct=sl_pct / 100.0,
        comm_pct=comm_pct / 100.0,
    )

    # ── Live RSI state — son kapanmis mumun (n-2) sonundaki RMA ─
    # Frontend live tick'te bu state'ten ileri sarak canli RSI hesaplar
    # Pine Script: live bar icin state = (bar_idx - 1) sonundaki RMA
    closes_all = [float(k[4]) for k in klines]
    rsi_state: dict[str, Any] | None = None
    if len(closes_all) >= rsi_len + 3:
        # Son mumu (canli) hariç tut → kapanmis mumlar uzerinden state
        _, _state = calculate_rsi_with_state(closes_all[:-1], rsi_len)
        rsi_state = {
            "avg_gain": _state["avg_gain"],
            "avg_loss": _state["avg_loss"],
            "prev_close": closes_all[-2],  # son kapanmis mum close'u
            "rsi_len": rsi_len,
        }
    result["rsi_state"] = rsi_state

    result["symbol"] = sym
    result["interval"] = interval
    result["params"] = {
        "rsi_len": rsi_len,
        "long_thresh": long_thresh,
        "short_thresh": short_thresh,
        "max_gap": max_gap,
        "entry_buffer_pct": entry_buffer_pct,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "comm_pct": comm_pct,
        "bars": bars,
    }
    return result


# ── Canli sinyal kayit endpoint'leri ─────────────────


@router.post("/api/pine-live-signal")
async def post_pine_live_signal(request: Request) -> dict:
    """Frontend pine_monitor.html'in canli mum tick'lerinden urettigi sinyali kaydet.

    Body (JSON):
    {
        "symbol": "MYXUSDT", "interval": "5m", "direction": "BUY",
        "entry_price": 0.2456, "tp_price": 0.2483, "sl_price": 0.2447,
        "rsi_a": 28.5, "rsi_b": 32.1, "gap": 5,
        "a_time": 1775654700, "bar_time": 1775658000, "tick_price": 0.2455,
        "rsi_len": 10, "long_thresh": 32, "short_thresh": 70, "max_gap": 12,
        "entry_buffer": 0.1, "tp_pct": 1.1, "sl_pct": 0.35
    }

    UNIQUE constraint: ayni (symbol, interval, bar_time, direction) bir kez kaydedilir.
    """
    try:
        body = await request.json()
    except Exception:
        return {"status": "error", "detail": "Invalid JSON"}

    if not body.get("symbol") or not body.get("direction") or not body.get("entry_price"):
        return {"status": "error", "detail": "Missing required fields"}

    if not body.get("created_at"):
        body["created_at"] = datetime.now(_TZ_IST).strftime("%Y-%m-%d %H:%M:%S")

    row_id = await insert_live_signal(body)
    return {"status": "ok", "id": row_id, "created_at": body["created_at"]}


@router.get("/api/pine-live-signals")
async def get_pine_live_signals(
    symbol: str | None = None,
    limit: int = 200,
) -> dict:
    """Kayitli canli sinyalleri dondur. Sembol filtresi opsiyonel."""
    limit = max(1, min(limit, 1000))
    rows = await query_live_signals(symbol=symbol, limit=limit)
    return {"signals": rows, "count": len(rows)}


# ── Pine Params (backend live engine icin) ──────────


@router.post("/api/pine-params")
async def post_pine_params(request: Request) -> dict:
    """Sembol+interval icin pine parametrelerini kaydet (frontend pine_monitor'den).

    Body: {symbol, interval, rsi_len, long_thresh, short_thresh, max_gap,
           entry_buffer, tp_pct, sl_pct, comm_pct, bars, active}
    """
    try:
        body = await request.json()
    except Exception:
        return {"status": "error", "detail": "Invalid JSON"}

    symbol = body.get("symbol", "").upper()
    interval = body.get("interval", "5m")
    if not symbol:
        return {"status": "error", "detail": "symbol required"}

    await upsert_params(symbol, interval, body)
    return {"status": "ok", "symbol": symbol, "interval": interval}


@router.get("/api/pine-params")
async def get_all_pine_params() -> dict:
    rows = await get_all_active_params()
    return {"params": rows, "count": len(rows)}


@router.get("/api/pine-params/{symbol}")
async def get_pine_params_for_symbol(symbol: str, interval: str = "5m") -> dict:
    row = await get_params(symbol, interval)
    return {"params": row}


@router.get("/api/engine-debug")
async def engine_debug() -> dict:
    """Motor crash debug — import ve task durumu."""
    result = {}
    try:
        from app.modules.ha_signal_engine import _ha_engine_task, _ha_engines
        result["ha_task_exists"] = _ha_engine_task is not None
        result["ha_task_done"] = _ha_engine_task.done() if _ha_engine_task else None
        if _ha_engine_task and _ha_engine_task.done():
            exc = _ha_engine_task.exception()
            result["ha_exception"] = str(exc) if exc else None
        result["ha_engines_count"] = len(_ha_engines)
    except Exception as e:
        result["ha_import_error"] = str(e)

    try:
        from app.modules.signal_engine import _engine_task, _engines
        result["engine_task_exists"] = _engine_task is not None
        result["engine_task_done"] = _engine_task.done() if _engine_task else None
        if _engine_task and _engine_task.done():
            exc = _engine_task.exception()
            result["engine_exception"] = str(exc) if exc else None
        result["engines_count"] = len(_engines)
    except Exception as e:
        result["engine_import_error"] = str(e)

    return result


@router.get("/api/account-b-test")
async def test_account_b() -> dict:
    """2. Binance hesabi baglanti testi."""
    from app.modules.binance_client import is_account_b_configured, get_total_wallet_balance_b
    if not is_account_b_configured():
        return {"status": "NOT_CONFIGURED", "msg": "BINANCE_API_KEY_B / BINANCE_API_SECRET_B ayarlanmamis"}
    try:
        balance = await get_total_wallet_balance_b()
        return {"status": "OK", "balance_b": balance}
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}


@router.get("/api/pine-live-engine-status")
async def get_pine_live_engine_status() -> dict:
    """Backend live engine'in mevcut durumu — debug icin."""
    from app.modules.pine_live_engine import get_all_states
    states = get_all_states()
    out = []
    for key, st in states.items():
        out.append({
            "symbol": st.symbol,
            "interval": st.interval,
            "warmed_up": st.warmed_up,
            "rsi_warmed_up": st.rsi_warmed_up,
            "closed_candles": len(st.closed_candles),
            "live_candle_time": st.live_candle.get("time") if st.live_candle else None,
            "live_close": st.live_candle.get("close") if st.live_candle else None,
            "signal_fired_this_bar": st.signal_fired_this_bar,
            "used_a_count": len(st.used_a),
            "params": st.params,
        })
    return {"engines": out, "count": len(out)}
