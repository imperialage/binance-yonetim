"""Backtest engine — Pine v3 "HA Renk + Prev Bar High/Low Limit" mantiginin
Python portu. TP/SL grid search ve tek-run backtest destegi.

Pine v3 mantigi:
  - Chart : normal mum (TF X)
  - Ust TF: HA mum (TF Y)
  - Sinyal:
      HTF HA yesil  -> LONG bekle (prevLow * (1-goodPct) limit)
      HTF HA kirmizi -> SHORT bekle (prevHigh * (1+goodPct) limit)
  - Alert filtresi (opsiyonel): 1-onceki HTF de ayni yon olmali
  - Backtest fill: bar range [low, high] limit fiyatina degdi ise
  - TP: entry * (1 ± tpPct), intra-bar wick tetik
  - SL: entry * (1 ∓ slPct), intra-bar wick tetik (SL oncelikli)
  - GUARD: bar_index > entryBar, exitedThisBar guard

Bu modul saf hesap yapar — httpx / asyncio dis kod cagirilmaz.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.modules.backtest_engine import INTERVAL_MS


# ── HA hesap ──────────────────────────────────────────────────────────

def compute_htf_ha_colors(htf_klines: list[list[Any]]) -> list[dict]:
    """HTF klines -> HA color listesi.

    Her HA bar: {open_time_ms, close_time_ms, ha_open, ha_close, is_green}
    """
    if not htf_klines:
        return []

    result: list[dict] = []
    prev_ha_open: float | None = None
    prev_ha_close: float | None = None

    for k in htf_klines:
        open_time_ms = int(k[0])
        o = float(k[1])
        h = float(k[2])
        l = float(k[3])
        c = float(k[4])
        close_time_ms = int(k[6]) if len(k) > 6 else open_time_ms

        ha_close = (o + h + l + c) / 4.0
        if prev_ha_open is None:
            ha_open = (o + c) / 2.0
        else:
            ha_open = (prev_ha_open + prev_ha_close) / 2.0

        result.append({
            "open_time_ms": open_time_ms,
            "close_time_ms": close_time_ms,
            "ha_open": ha_open,
            "ha_close": ha_close,
            "is_green": ha_close >= ha_open,
        })

        prev_ha_open = ha_open
        prev_ha_close = ha_close

    return result


def _binary_search_last_closed_htf(htf_bars: list[dict], chart_time_ms: int) -> int:
    """chart_time_ms zamanina kadar KAPANMIS son HTF bar'in index'i.
    -1 = henuz kapanmis HTF yok. Binary search O(log n).
    """
    lo, hi = 0, len(htf_bars) - 1
    ans = -1
    while lo <= hi:
        mid = (lo + hi) // 2
        # HTF bar kapanis time: bir sonraki bar'in acilis time'i - 1
        # Pratikte close_time_ms + 1 sonraki bar acilis
        # lookahead_off: kapanis time <= chart_time_ms olan son bar
        # close_time_ms = ozgun bar kapanis (dahil son ms), yani < chart_time_ms
        htf_close_ms = htf_bars[mid]["close_time_ms"]
        if htf_close_ms < chart_time_ms:
            ans = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return ans


# ── Backtest ana loop ─────────────────────────────────────────────────

@dataclass
class BacktestParams:
    tp_pct: float          # 0.01 = %1
    sl_pct: float          # 0.005 = %0.5
    use_good: bool = False
    good_pct: float = 0.005
    require_prev_htf: bool = False  # v3 alert filtresi
    commission_pct: float = 0.0008  # 0.08% round-trip


def run_v3_backtest(
    chart_klines: list[list[Any]],
    htf_klines: list[list[Any]],
    params: BacktestParams,
    start_ms: int,
) -> dict:
    """Pine v3 mantigiyla backtest.

    Args:
        chart_klines: chart TF ham kline listesi (warmup dahil)
        htf_klines: HTF ham kline listesi (warmup dahil)
        params: TP/SL/tolerans/filtre parametreleri
        start_ms: backtest baslangic (warmup hariç sonuclar)

    Returns:
        {
            "trades": [...],
            "summary": {n_trades, n_win, n_loss, wr, pf, net_pnl_pct,
                        avg_win_pct, avg_loss_pct, expectancy_pct,
                        max_dd_pct, n_tp, n_sl},
            "n_bars_skipped_prev_htf": int,  # filtre nedeniyle skip
        }
    """
    htf_bars = compute_htf_ha_colors(htf_klines)
    if not htf_bars:
        return _empty_result("HTF veri bos")
    if not chart_klines:
        return _empty_result("Chart veri bos")

    tp_pct = params.tp_pct
    sl_pct = params.sl_pct
    good_pct = params.good_pct if params.use_good else 0.0
    comm = params.commission_pct

    # State
    state = 0  # 0=FLAT, 1=LONG, -1=SHORT
    entry_price = 0.0
    entry_bar_idx = -1
    entry_bar_time = 0

    # Metrics
    trades: list[dict] = []
    equity = 1.0  # normalize (%1 = 0.01)
    peak = 1.0
    max_dd = 0.0

    n_skipped_prev_htf = 0

    for i, k in enumerate(chart_klines):
        chart_time = int(k[0])
        o = float(k[1])
        h = float(k[2])
        l = float(k[3])
        c = float(k[4])

        # Backtest start ms'ten oncekileri sadece prev_low/high olarak kullan
        if chart_time < start_ms:
            continue

        # prev bar (i-1)
        if i == 0:
            continue
        prev_k = chart_klines[i - 1]
        prev_high = float(prev_k[2])
        prev_low = float(prev_k[3])

        # HTF renk (son kapanmis) + 1 onceki HTF renk
        htf_idx = _binary_search_last_closed_htf(htf_bars, chart_time)
        if htf_idx < 0:
            continue  # henuz HTF kapanmadi
        htf_green = htf_bars[htf_idx]["is_green"]
        htf_prev_green: bool | None = None
        if htf_idx >= 1:
            htf_prev_green = htf_bars[htf_idx - 1]["is_green"]

        long_order = prev_low * (1.0 - good_pct)
        short_order = prev_high * (1.0 + good_pct)

        exited_this_bar = False

        # ── 1) SL kontrolu (oncelikli) ──
        if state == 1 and i > entry_bar_idx:
            sl_price = entry_price * (1.0 - sl_pct)
            if l <= sl_price:
                pct = (sl_price - entry_price) / entry_price - comm
                _close_trade(trades, "LONG", "SL", entry_price, sl_price,
                             entry_bar_time, chart_time, pct)
                equity *= (1.0 + pct)
                peak = max(peak, equity)
                dd = (peak - equity) / peak
                if dd > max_dd:
                    max_dd = dd
                state = 0
                exited_this_bar = True
                continue
        elif state == -1 and i > entry_bar_idx:
            sl_price = entry_price * (1.0 + sl_pct)
            if h >= sl_price:
                pct = (entry_price - sl_price) / entry_price - comm
                _close_trade(trades, "SHORT", "SL", entry_price, sl_price,
                             entry_bar_time, chart_time, pct)
                equity *= (1.0 + pct)
                peak = max(peak, equity)
                dd = (peak - equity) / peak
                if dd > max_dd:
                    max_dd = dd
                state = 0
                exited_this_bar = True
                continue

        # ── 2) TP kontrolu (SL tetiklenmediyse) ──
        if state == 1 and i > entry_bar_idx:
            tp_price = entry_price * (1.0 + tp_pct)
            if h >= tp_price:
                pct = (tp_price - entry_price) / entry_price - comm
                _close_trade(trades, "LONG", "TP", entry_price, tp_price,
                             entry_bar_time, chart_time, pct)
                equity *= (1.0 + pct)
                peak = max(peak, equity)
                dd = (peak - equity) / peak
                if dd > max_dd:
                    max_dd = dd
                state = 0
                exited_this_bar = True
                continue
        elif state == -1 and i > entry_bar_idx:
            tp_price = entry_price * (1.0 - tp_pct)
            if l <= tp_price:
                pct = (entry_price - tp_price) / entry_price - comm
                _close_trade(trades, "SHORT", "TP", entry_price, tp_price,
                             entry_bar_time, chart_time, pct)
                equity *= (1.0 + pct)
                peak = max(peak, equity)
                dd = (peak - equity) / peak
                if dd > max_dd:
                    max_dd = dd
                state = 0
                exited_this_bar = True
                continue

        # ── 3) LIMIT emir (FLAT + HTF sinyali) ──
        if state == 0 and not exited_this_bar:
            # Alert filtresi (opsiyonel — sadece istatistik icin)
            if params.require_prev_htf and htf_prev_green is not None:
                allow_long = htf_green and htf_prev_green
                allow_short = (not htf_green) and (not htf_prev_green)
            else:
                allow_long = htf_green
                allow_short = not htf_green

            # HTF YESIL -> LONG bekle
            if allow_long:
                if l <= long_order <= h:
                    state = 1
                    entry_price = long_order
                    entry_bar_idx = i
                    entry_bar_time = chart_time
                elif htf_green and not allow_long:
                    n_skipped_prev_htf += 1

            # HTF KIRMIZI -> SHORT bekle
            if state == 0 and allow_short:
                if l <= short_order <= h:
                    state = -1
                    entry_price = short_order
                    entry_bar_idx = i
                    entry_bar_time = chart_time
            elif state == 0 and (not htf_green) and (not allow_short):
                n_skipped_prev_htf += 1

    # Ozet
    n_trades = len(trades)
    n_win = sum(1 for t in trades if t["pct"] > 0)
    n_loss = n_trades - n_win
    n_tp = sum(1 for t in trades if t["exit_type"] == "TP")
    n_sl = sum(1 for t in trades if t["exit_type"] == "SL")

    sum_win_pct = sum(t["pct"] for t in trades if t["pct"] > 0)
    sum_loss_pct = sum(t["pct"] for t in trades if t["pct"] <= 0)
    net_pnl_pct = (equity - 1.0)

    wr = (n_win / n_trades * 100.0) if n_trades > 0 else 0.0
    pf = (sum_win_pct / abs(sum_loss_pct)) if sum_loss_pct != 0 else 0.0
    avg_win = (sum_win_pct / n_win) if n_win > 0 else 0.0
    avg_loss = (sum_loss_pct / n_loss) if n_loss > 0 else 0.0
    expectancy = ((sum_win_pct + sum_loss_pct) / n_trades) if n_trades > 0 else 0.0

    return {
        "trades": trades,
        "summary": {
            "n_trades": n_trades,
            "n_win": n_win,
            "n_loss": n_loss,
            "n_tp": n_tp,
            "n_sl": n_sl,
            "wr_pct": round(wr, 2),
            "pf": round(pf, 3),
            "net_pnl_pct": round(net_pnl_pct * 100, 3),
            "avg_win_pct": round(avg_win * 100, 3),
            "avg_loss_pct": round(avg_loss * 100, 3),
            "expectancy_pct": round(expectancy * 100, 4),
            "max_dd_pct": round(max_dd * 100, 3),
        },
        "n_bars_skipped_prev_htf_filter": n_skipped_prev_htf,
    }


def _close_trade(trades, side, exit_type, entry, exit_px, ent_time, exit_time, pct):
    trades.append({
        "side": side,
        "exit_type": exit_type,
        "entry": entry,
        "exit": exit_px,
        "entry_time_ms": ent_time,
        "exit_time_ms": exit_time,
        "pct": pct,
    })


def _empty_result(reason: str) -> dict:
    return {
        "trades": [],
        "summary": {
            "n_trades": 0, "n_win": 0, "n_loss": 0, "n_tp": 0, "n_sl": 0,
            "wr_pct": 0.0, "pf": 0.0, "net_pnl_pct": 0.0,
            "avg_win_pct": 0.0, "avg_loss_pct": 0.0,
            "expectancy_pct": 0.0, "max_dd_pct": 0.0,
        },
        "n_bars_skipped_prev_htf_filter": 0,
        "note": reason,
    }


# ── Grid search ───────────────────────────────────────────────────────

def run_v3_grid(
    chart_klines: list[list[Any]],
    htf_klines: list[list[Any]],
    start_ms: int,
    tp_range: list[float],
    sl_range: list[float],
    use_good: bool = False,
    good_pct: float = 0.005,
    require_prev_htf: bool = False,
    commission_pct: float = 0.0008,
) -> dict:
    """TP × SL grid backtest. Her kombinasyon icin ozet.

    Returns:
        {
            "grid": [
                {"tp_pct": 0.005, "sl_pct": 0.003, "summary": {...}},
                ...
            ],
            "best_by_net_pnl": {...},
            "best_by_pf": {...},
            "best_by_expectancy": {...},
        }
    """
    grid_results: list[dict] = []

    # HTF bar hesap 1 kere yap — grid'de cache
    for tp in tp_range:
        for sl in sl_range:
            params = BacktestParams(
                tp_pct=tp,
                sl_pct=sl,
                use_good=use_good,
                good_pct=good_pct,
                require_prev_htf=require_prev_htf,
                commission_pct=commission_pct,
            )
            res = run_v3_backtest(chart_klines, htf_klines, params, start_ms)
            grid_results.append({
                "tp_pct": round(tp * 100, 3),
                "sl_pct": round(sl * 100, 3),
                "summary": res["summary"],
            })

    # En iyiler
    best_pnl = max(grid_results,
                   key=lambda r: r["summary"]["net_pnl_pct"],
                   default=None)
    best_pf = max(grid_results,
                  key=lambda r: r["summary"]["pf"],
                  default=None)
    best_exp = max(grid_results,
                   key=lambda r: r["summary"]["expectancy_pct"],
                   default=None)

    return {
        "grid": grid_results,
        "best_by_net_pnl": best_pnl,
        "best_by_pf": best_pf,
        "best_by_expectancy": best_exp,
    }
