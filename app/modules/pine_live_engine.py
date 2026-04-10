"""Pine Live Engine — sembol bazli sürekli calisan backend live divergence engine.

Bu engine signal_engine.py'den BAGIMSIZDIR. Trade ACMAZ. Sadece pine_monitor
sayfasinin parametreleri ile Pine Script Hidden RSI Divergence sinyallerini
canli olarak yakalar ve pine_live_signals tablosuna yazar.

Calisma:
- Her sembol icin closed_candles + live_candle + rsiState (per interval)
- Her saniyede bir tum sembolleri tarar (price_stream'den canli fiyat)
- Yeni mum baslayinca state ileri sarar (Wilder RMA)
- Live divergence check (Pine Script ile birebir)
- Sinyal yakalanirsa pine_live_signals'a yazar (UNIQUE constraint duplike onler)
- 5 dakikada bir Binance'tan klines drift fix
- 30 saniyede bir aktif sembol/parametre listesi yenilenir
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx

from app.modules.price_stream import get_live_price
from app.modules.rsi_calculator import calculate_rsi_with_state
from app.modules.pine_live_store import (
    get_all_active_params,
    insert_live_signal,
)
from app.utils.logging import get_logger

log = get_logger(__name__)

_TZ_IST = timezone(timedelta(hours=3))
_BINANCE_URL = "https://fapi.binance.com/fapi/v1/klines"

INTERVAL_SEC = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900,
    "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400,
    "6h": 21600, "12h": 43200, "1d": 86400,
}


class PineLiveSymbolState:
    """Bir sembol+interval icin canli takip state."""

    def __init__(self, symbol: str, interval: str, params: dict):
        self.symbol = symbol
        self.interval = interval
        self.params = params
        self.iv_sec = INTERVAL_SEC.get(interval, 300)

        # Mum verileri
        self.closed_candles: list[dict] = []  # {time, open, high, low, close, rsi}
        self.live_candle: dict | None = None  # {time, open, high, low, close}

        # RSI state — Wilder RMA
        self.avg_gain: float = 0.0
        self.avg_loss: float = 0.0
        self.prev_close: float = 0.0
        self.rsi_warmed_up: bool = False

        # Pine Script state
        self.used_a: set[int] = set()  # bar zamanlari (epoch)
        self.signal_fired_this_bar: bool = False

        self.warmed_up: bool = False
        self.last_warmup: float = 0.0

    def update_params(self, params: dict) -> None:
        """Parametreler degisirse state'i sifirla (RSI uzunlugu/esik degismis olabilir)."""
        if (params.get("rsi_len") != self.params.get("rsi_len")
                or params.get("max_gap") != self.params.get("max_gap")):
            # RSI len degistiyse warmup gerek
            self.warmed_up = False
        self.params = params

    async def warmup(self) -> None:
        """Binance'tan klines cek → RSI hesapla → state hazirla."""
        try:
            rsi_len = int(self.params.get("rsi_len", 10))
            bars = int(self.params.get("bars", 500))
            limit = max(bars + rsi_len + 20, 100)
            limit = min(limit, 1500)

            proxy_url = None
            try:
                from app.config import settings as app_settings
                proxy_url = app_settings.binance_proxy_url or None
            except Exception:
                pass

            async with httpx.AsyncClient(timeout=15, proxy=proxy_url) as client:
                resp = await client.get(_BINANCE_URL, params={
                    "symbol": self.symbol,
                    "interval": self.interval,
                    "limit": limit,
                })
                resp.raise_for_status()
                klines = resp.json()

            if not klines or len(klines) < rsi_len + 3:
                return

            closes = [float(k[4]) for k in klines]
            opens = [float(k[1]) for k in klines]
            highs = [float(k[2]) for k in klines]
            lows = [float(k[3]) for k in klines]
            times = [int(k[0]) // 1000 for k in klines]

            # RSI tum closes uzerinde (son mum dahil)
            from app.modules.rsi_calculator import calculate_rsi
            rsi_values = calculate_rsi(closes, rsi_len)

            # Closed candles = son mum haric
            self.closed_candles = []
            for i in range(len(klines) - 1):
                self.closed_candles.append({
                    "time": times[i],
                    "open": opens[i],
                    "high": highs[i],
                    "low": lows[i],
                    "close": closes[i],
                    "rsi": rsi_values[i] if i < len(rsi_values) else None,
                })

            # Live mum = son mum
            self.live_candle = {
                "time": times[-1],
                "open": opens[-1],
                "high": highs[-1],
                "low": lows[-1],
                "close": closes[-1],
            }

            # RSI state — closed mumlar uzerinden (live haric)
            _, state = calculate_rsi_with_state(closes[:-1], rsi_len)
            self.avg_gain = state.get("avg_gain", 0.0)
            self.avg_loss = state.get("avg_loss", 0.0)
            self.prev_close = closes[-2] if len(closes) >= 2 else 0.0
            self.rsi_warmed_up = True

            self.warmed_up = True
            self.last_warmup = time.time()
            await log.ainfo(
                "pine_live_warmup",
                symbol=self.symbol, interval=self.interval,
                closed=len(self.closed_candles),
            )
        except Exception as e:
            await log.aerror("pine_live_warmup_error", symbol=self.symbol, error=str(e))

    def calc_live_rsi(self, price: float) -> float | None:
        """Wilder RMA bir adim ileri — Pine Script live bar gibi."""
        if not self.rsi_warmed_up or self.prev_close == 0:
            return None
        rsi_len = int(self.params.get("rsi_len", 10))
        delta = price - self.prev_close
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        ag = (self.avg_gain * (rsi_len - 1) + gain) / rsi_len
        al = (self.avg_loss * (rsi_len - 1) + loss) / rsi_len
        if al == 0:
            return 100.0
        rs = ag / al
        return 100.0 - 100.0 / (1.0 + rs)

    def advance_candle(self, new_bar_start: int, price: float) -> None:
        """Yeni mum basladi → live mumu kapanmis isle, state ileri sar."""
        if not self.live_candle:
            return
        lc = self.live_candle
        rsi_len = int(self.params.get("rsi_len", 10))

        # Wilder RMA bir adim ileri
        delta = lc["close"] - self.prev_close
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        new_ag = (self.avg_gain * (rsi_len - 1) + gain) / rsi_len
        new_al = (self.avg_loss * (rsi_len - 1) + loss) / rsi_len
        rsi_val = 100.0 if new_al == 0 else (100.0 - 100.0 / (1.0 + new_ag / new_al))

        # Kapanan mumu listeye ekle
        self.closed_candles.append({
            "time": lc["time"],
            "open": lc["open"],
            "high": lc["high"],
            "low": lc["low"],
            "close": lc["close"],
            "rsi": rsi_val,
        })
        if len(self.closed_candles) > 500:
            self.closed_candles = self.closed_candles[-500:]

        # State ileri sar
        self.avg_gain = new_ag
        self.avg_loss = new_al
        self.prev_close = lc["close"]

        # Yeni live mum
        self.live_candle = {
            "time": new_bar_start,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
        }
        self.signal_fired_this_bar = False

    def check_live_divergence(self) -> dict | None:
        """Pine Script Hidden RSI Divergence — live tick check."""
        if self.signal_fired_this_bar or not self.live_candle:
            return None
        if not self.closed_candles:
            return None

        live_rsi = self.calc_live_rsi(self.live_candle["close"])
        if live_rsi is None:
            return None

        p = self.params
        n = len(self.closed_candles)
        max_gap = min(int(p.get("max_gap", 12)), n)
        long_thresh = float(p.get("long_thresh", 32))
        short_thresh = float(p.get("short_thresh", 70))
        entry_buffer = float(p.get("entry_buffer", 0.1)) / 100.0

        live = self.live_candle

        # SHORT (Pine: once SHORT)
        for i in range(1, max_gap + 1):
            ai = n - i
            if ai < 0:
                continue
            a = self.closed_candles[ai]
            if a.get("rsi") is None:
                continue
            if a["time"] in self.used_a:
                continue
            if (a["rsi"] >= short_thresh
                    and live["high"] > a["high"]
                    and live_rsi < a["rsi"]):
                return {
                    "direction": "SELL",
                    "entry_price": live["high"] * (1 - entry_buffer),
                    "rsi_a": round(a["rsi"], 2),
                    "rsi_b": round(live_rsi, 2),
                    "gap": i,
                    "a_time": a["time"],
                    "bar_time": live["time"],
                    "tick_price": live["close"],
                }

        # LONG
        for j in range(1, max_gap + 1):
            ai = n - j
            if ai < 0:
                continue
            a = self.closed_candles[ai]
            if a.get("rsi") is None:
                continue
            if a["time"] in self.used_a:
                continue
            if (a["rsi"] <= long_thresh
                    and live["low"] < a["low"]
                    and live_rsi > a["rsi"]):
                return {
                    "direction": "BUY",
                    "entry_price": live["low"] * (1 + entry_buffer),
                    "rsi_a": round(a["rsi"], 2),
                    "rsi_b": round(live_rsi, 2),
                    "gap": j,
                    "a_time": a["time"],
                    "bar_time": live["time"],
                    "tick_price": live["close"],
                }
        return None


# ── Engine state ─────────────────────────────────────

# (symbol, interval) → PineLiveSymbolState
_states: dict[tuple[str, str], PineLiveSymbolState] = {}
_engine_task: asyncio.Task | None = None


def get_state(symbol: str, interval: str) -> PineLiveSymbolState | None:
    return _states.get((symbol.upper(), interval))


def get_all_states() -> dict:
    return {f"{k[0]}_{k[1]}": v for k, v in _states.items()}


async def _process_tick(state: PineLiveSymbolState) -> None:
    """Bir sembol icin tick isle — yeni mum check + live divergence."""
    if not state.warmed_up or not state.live_candle:
        return
    price = get_live_price(state.symbol)
    if price is None or price <= 0:
        return

    now = int(time.time())
    expected_bar_start = (now // state.iv_sec) * state.iv_sec

    # Yeni mum basladi mi?
    if expected_bar_start > state.live_candle["time"] and now - expected_bar_start > 2:
        state.advance_candle(expected_bar_start, price)

    # Live mumu guncelle
    lc = state.live_candle
    lc["high"] = max(lc["high"], price)
    lc["low"] = min(lc["low"], price)
    lc["close"] = price

    # Live divergence check
    sig = state.check_live_divergence()
    if not sig:
        return

    state.signal_fired_this_bar = True
    state.used_a.add(sig["a_time"])

    # Sinyali DB'ye yaz
    p = state.params
    direction = sig["direction"]
    entry = sig["entry_price"]
    tp_pct = float(p.get("tp_pct", 1.1)) / 100.0
    sl_pct = float(p.get("sl_pct", 0.35)) / 100.0
    if direction == "BUY":
        tp_price = entry * (1 + tp_pct)
        sl_price = entry * (1 - sl_pct)
    else:
        tp_price = entry * (1 - tp_pct)
        sl_price = entry * (1 + sl_pct)

    record = {
        "created_at": datetime.now(_TZ_IST).strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": state.symbol,
        "interval": state.interval,
        "direction": direction,
        "entry_price": entry,
        "tp_price": tp_price,
        "sl_price": sl_price,
        "rsi_a": sig["rsi_a"],
        "rsi_b": sig["rsi_b"],
        "gap": sig["gap"],
        "a_time": sig["a_time"],
        "bar_time": sig["bar_time"],
        "tick_price": sig["tick_price"],
        "rsi_len": int(p.get("rsi_len", 10)),
        "long_thresh": float(p.get("long_thresh", 32)),
        "short_thresh": float(p.get("short_thresh", 70)),
        "max_gap": int(p.get("max_gap", 12)),
        "entry_buffer": float(p.get("entry_buffer", 0.1)),
        "tp_pct": float(p.get("tp_pct", 1.1)),
        "sl_pct": float(p.get("sl_pct", 0.35)),
    }
    await insert_live_signal(record)
    await log.ainfo(
        "pine_live_signal_captured",
        symbol=state.symbol, direction=direction,
        entry=round(entry, 6), rsi_a=sig["rsi_a"], rsi_b=sig["rsi_b"], gap=sig["gap"],
    )


async def _sync_active_symbols() -> None:
    """Aktif sembol/parametre listesini DB'den yenile."""
    try:
        active = await get_all_active_params()
    except Exception as e:
        await log.aerror("pine_live_sync_error", error=str(e))
        return

    active_keys = set()
    for row in active:
        sym = row["symbol"]
        iv = row["interval"]
        key = (sym, iv)
        active_keys.add(key)
        params = {k: row[k] for k in row.keys() if k not in ("symbol", "interval", "active", "updated_at")}
        if key not in _states:
            state = PineLiveSymbolState(sym, iv, params)
            await state.warmup()
            if state.warmed_up:
                _states[key] = state
                await log.ainfo("pine_live_engine_added", symbol=sym, interval=iv)
        else:
            _states[key].update_params(params)
            if not _states[key].warmed_up:
                await _states[key].warmup()

    # Aktif olmayan sembolleri kaldir
    for key in list(_states.keys()):
        if key not in active_keys:
            del _states[key]
            await log.ainfo("pine_live_engine_removed", symbol=key[0], interval=key[1])


async def _pine_live_loop() -> None:
    """Ana loop — saniyede bir tum sembolleri tarar."""
    await log.ainfo("pine_live_engine_loop_starting")

    last_sync = 0.0
    last_warmup_refresh = time.time()

    try:
        while True:
            await asyncio.sleep(1)
            now = time.time()

            # 30sn'de bir aktif sembol/parametre listesini yenile
            if now - last_sync > 30:
                last_sync = now
                await _sync_active_symbols()

            # 5dk'da bir tum semboller icin warmup refresh (drift fix)
            if now - last_warmup_refresh > 300:
                last_warmup_refresh = now
                for state in list(_states.values()):
                    try:
                        await state.warmup()
                    except Exception as e:
                        await log.awarning("pine_live_refresh_error", symbol=state.symbol, error=str(e))

            # Her sembol icin tick isle
            for state in list(_states.values()):
                try:
                    await _process_tick(state)
                except Exception as e:
                    await log.awarning("pine_live_tick_error", symbol=state.symbol, error=str(e))

    except asyncio.CancelledError:
        await log.ainfo("pine_live_engine_loop_stopped")


def start_pine_live_engine() -> None:
    global _engine_task
    _engine_task = asyncio.create_task(_pine_live_loop())


async def stop_pine_live_engine() -> None:
    global _engine_task
    if _engine_task:
        _engine_task.cancel()
        try:
            await _engine_task
        except asyncio.CancelledError:
            pass
        _engine_task = None
