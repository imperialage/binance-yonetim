"""Heikin-Ashi Signal Engine — MYXUSDT icin ozel Hidden RSI Divergence motoru.

Normal mumlar yerine Heikin-Ashi mumlari uzerinden RSI hesaplar.
Tum trade altyapisi (trade_executor, order_stream, TP/SL) aynen kullanilir.

HA Formulleri:
  HA_Close = (O + H + L + C) / 4
  HA_Open  = (prev_HA_Open + prev_HA_Close) / 2
  HA_High  = max(H, HA_Open, HA_Close)
  HA_Low   = min(L, HA_Open, HA_Close)
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

from app.modules.rsi_calculator import calculate_rsi
from app.modules.indicator_settings_store import get_settings_or_defaults
from app.modules.binance_client import get_position_risk
from app.modules.price_stream import get_live_price
from app.utils.logging import get_logger

log = get_logger(__name__)

_DATA_DIR = os.getenv("DATA_DIR", "data")
BINANCE_URL = "https://fapi.binance.com/fapi/v1/klines"

INTERVAL_SECONDS = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "1d": 86400,
}

# HA engine registry
_ha_engines: dict[str, HeikinAshiEngine] = {}
_ha_engine_task: asyncio.Task | None = None


# ── Heikin-Ashi donusum ────────────────────────────────


def convert_klines_to_ha(klines: list) -> list[dict]:
    """Normal Binance klines → Heikin-Ashi mumlari.

    klines: [[open_time, O, H, L, C, V, ...], ...]
    Returns: [{"time":..,"open":..,"high":..,"low":..,"close":..}, ...]
    """
    if not klines:
        return []

    result = []
    prev_ha_open = float(klines[0][1])
    prev_ha_close = (float(klines[0][1]) + float(klines[0][2]) + float(klines[0][3]) + float(klines[0][4])) / 4

    for k in klines:
        o = float(k[1])
        h = float(k[2])
        l = float(k[3])  # noqa: E741
        c = float(k[4])

        ha_close = (o + h + l + c) / 4
        ha_open = (prev_ha_open + prev_ha_close) / 2
        ha_high = max(h, ha_open, ha_close)
        ha_low = min(l, ha_open, ha_close)

        result.append({
            "time": int(k[0]) // 1000,
            "open": ha_open,
            "high": ha_high,
            "low": ha_low,
            "close": ha_close,
            # Gercek fiyatlar (entry price hesabi icin)
            "real_high": h,
            "real_low": l,
            "real_close": c,
        })

        prev_ha_open = ha_open
        prev_ha_close = ha_close

    return result


def calc_ha_candle(o: float, h: float, l: float, c: float,
                   prev_ha_open: float, prev_ha_close: float) -> dict:
    """Tek mum icin HA hesapla."""
    ha_close = (o + h + l + c) / 4
    ha_open = (prev_ha_open + prev_ha_close) / 2
    ha_high = max(h, ha_open, ha_close)
    ha_low = min(l, ha_open, ha_close)
    return {
        "ha_open": ha_open,
        "ha_high": ha_high,
        "ha_low": ha_low,
        "ha_close": ha_close,
    }


# ── HeikinAshiEngine ──────────────────────────────────


class HeikinAshiEngine:
    """MYXUSDT icin Heikin-Ashi bazli Hidden RSI Divergence motoru."""

    def __init__(self, symbol: str, settings: dict[str, Any]):
        self.symbol = symbol
        self.settings = settings
        self.interval = settings.get("interval", "5m")
        self.iv_sec = INTERVAL_SECONDS.get(self.interval, 300)

        # Indikator parametreleri
        self.rsi_len: int = settings.get("rsi_len", 10)
        self.long_thresh: float = settings.get("long_thresh", 32.0)
        self.short_thresh: float = settings.get("short_thresh", 70.0)
        self.max_gap: int = settings.get("max_gap", 21)
        self.entry_buffer: float = settings.get("entry_buffer", 0.1) / 100.0

        # HA state
        self.ha_prev_open: float = 0.0
        self.ha_prev_close: float = 0.0

        # Kapanmis HA mumlari + RSI
        self.closed_candles: list[dict] = []

        # Canli mum state (normal OHLC — tick'ten)
        self.candle_start: int = 0
        self.candle_open: float = 0.0
        self.candle_high: float = 0.0
        self.candle_low: float = float("inf")
        self.candle_close: float = 0.0

        # Canli HA mum (normal mumdan hesaplanan)
        self.ha_candle_open: float = 0.0
        self.ha_candle_high: float = 0.0
        self.ha_candle_low: float = 0.0
        self.ha_candle_close: float = 0.0

        # RSI state (Wilder's RMA — HA close uzerinden)
        self.rsi_avg_gain: float = 0.0
        self.rsi_avg_loss: float = 0.0
        self.rsi_prev_close: float = 0.0  # onceki HA close
        self.rsi_warmed_up: bool = False

        # Pozisyon state
        self.has_position: bool = False
        self.position_side: str = ""

        # Sinyal state
        self.signal_fired_this_bar: bool = False
        self.trade_pending: bool = False
        self.used_a: set[int] = set()
        self.warmed_up: bool = False
        self.last_signal_time: float = 0.0
        self.last_signal: dict | None = None
        self.last_signal_bar: int = 0

        # TP/SL teyit
        self.tp_confirmed: bool = False
        self.sl_confirmed: bool = False
        self.tp_price: float = 0.0
        self.sl_price: float = 0.0

        # Entry bilgileri
        self.entry_price: float = 0.0
        self.entry_time: int = 0
        self.entry_qty: float = 0.0
        self.entry_event_id: str = ""
        self.entry_signal_id: int | None = None

        # Per-engine lock
        self._state_lock: asyncio.Lock = asyncio.Lock()

        # Pending order
        self.pending_order: dict | None = None

    # ── Warmup ──────────────────────────────────────────
    async def warmup(self) -> None:
        """Binance'tan 200 normal mum cek → HA'ya donustur → RSI hesapla."""
        import httpx

        try:
            proxy_url = None
            try:
                from app.config import settings as app_settings
                proxy_url = app_settings.binance_proxy_url or None
            except Exception:
                pass

            async with httpx.AsyncClient(timeout=15, proxy=proxy_url) as client:
                resp = await client.get(BINANCE_URL, params={
                    "symbol": self.symbol,
                    "interval": self.interval,
                    "limit": 200,
                })
                resp.raise_for_status()
                klines = resp.json()

            if not klines or len(klines) < self.rsi_len + 2:
                await log.awarning("ha_engine_warmup_insufficient", symbol=self.symbol, count=len(klines) if klines else 0)
                return

            # Normal klines → HA mumlari
            ha_candles = convert_klines_to_ha(klines)

            # HA close'lardan RSI hesapla
            ha_closes = [c["close"] for c in ha_candles]
            rsi_values = calculate_rsi(ha_closes, self.rsi_len)

            # Kapanmis mumlari doldur (son mum haric — o canli)
            self.closed_candles = []
            for i in range(len(ha_candles) - 1):
                hc = ha_candles[i]
                hc["rsi"] = rsi_values[i] if i < len(rsi_values) else None
                self.closed_candles.append(hc)

            # RSI state — son kapanmis HA mumun state'i
            n = len(ha_closes) - 1
            if n > self.rsi_len:
                gains = [0.0] * n
                losses = [0.0] * n
                for i in range(1, n):
                    d = ha_closes[i] - ha_closes[i - 1]
                    gains[i] = max(d, 0.0)
                    losses[i] = max(-d, 0.0)

                self.rsi_avg_gain = sum(gains[1:self.rsi_len + 1]) / self.rsi_len
                self.rsi_avg_loss = sum(losses[1:self.rsi_len + 1]) / self.rsi_len

                for i in range(self.rsi_len + 1, n):
                    self.rsi_avg_gain = (self.rsi_avg_gain * (self.rsi_len - 1) + gains[i]) / self.rsi_len
                    self.rsi_avg_loss = (self.rsi_avg_loss * (self.rsi_len - 1) + losses[i]) / self.rsi_len

                self.rsi_prev_close = ha_closes[n - 1]
                self.rsi_warmed_up = True

            # HA state kaydet (son kapanmis mum)
            if self.closed_candles:
                last_closed = self.closed_candles[-1]
                self.ha_prev_open = last_closed["open"]
                self.ha_prev_close = last_closed["close"]

            # Canli mumu baslat (normal OHLC)
            last = klines[-1]
            self.candle_start = int(last[0]) // 1000
            self.candle_open = float(last[1])
            self.candle_high = float(last[2])
            self.candle_low = float(last[3])
            self.candle_close = float(last[4])

            # Canli HA mumu hesapla
            self._update_ha_candle()

            # used_a yukle
            self._load_used_a()

            # Pozisyon kontrol
            await self._sync_position()

            self.warmed_up = True
            await log.ainfo(
                "ha_engine_warmup_done",
                symbol=self.symbol,
                interval=self.interval,
                candles=len(self.closed_candles),
                has_position=self.has_position,
                ha_prev_close=round(self.ha_prev_close, 4),
            )

        except Exception as e:
            await log.aerror("ha_engine_warmup_error", symbol=self.symbol, error=str(e))

    # ── HA mum guncelleme ───────────────────────────────
    def _update_ha_candle(self) -> None:
        """Normal canli OHLC'den HA canli mumu hesapla."""
        ha = calc_ha_candle(
            self.candle_open, self.candle_high, self.candle_low, self.candle_close,
            self.ha_prev_open, self.ha_prev_close,
        )
        self.ha_candle_open = ha["ha_open"]
        self.ha_candle_high = ha["ha_high"]
        self.ha_candle_low = ha["ha_low"]
        self.ha_candle_close = ha["ha_close"]

    # ── RSI (HA close uzerinden) ────────────────────────
    def _calc_live_rsi(self, ha_close: float) -> float | None:
        if not self.rsi_warmed_up:
            return None
        d = ha_close - self.rsi_prev_close
        g = max(d, 0.0)
        l_val = max(-d, 0.0)
        ag = (self.rsi_avg_gain * (self.rsi_len - 1) + g) / self.rsi_len
        al = (self.rsi_avg_loss * (self.rsi_len - 1) + l_val) / self.rsi_len
        if al == 0:
            return 100.0
        return round(100.0 - (100.0 / (1.0 + ag / al)), 2)

    def _advance_candle(self, ha_close: float) -> None:
        if not self.rsi_warmed_up:
            return
        d = ha_close - self.rsi_prev_close
        g = max(d, 0.0)
        l_val = max(-d, 0.0)
        self.rsi_avg_gain = (self.rsi_avg_gain * (self.rsi_len - 1) + g) / self.rsi_len
        self.rsi_avg_loss = (self.rsi_avg_loss * (self.rsi_len - 1) + l_val) / self.rsi_len
        self.rsi_prev_close = ha_close

    # ── Pozisyon sync ───────────────────────────────────
    async def _sync_position(self) -> None:
        try:
            positions = await get_position_risk(self.symbol)
            for p in positions:
                if p.get("symbol") == self.symbol:
                    amt = float(p.get("positionAmt", 0))
                    if amt > 0:
                        self.has_position = True
                        self.position_side = "LONG"
                    elif amt < 0:
                        self.has_position = True
                        self.position_side = "SHORT"
                    else:
                        self.has_position = False
                        self.position_side = ""
                        if not self.pending_order:
                            self.trade_pending = False
                    return
            self.has_position = False
            self.position_side = ""
            if not self.pending_order:
                self.trade_pending = False
        except Exception as e:
            await log.awarning("ha_engine_position_check_error", symbol=self.symbol, error=str(e))

    # ── Pozisyon kapandi ────────────────────────────────
    def on_position_closed(self, exit_info: dict | None = None) -> None:
        prev_direction = self.position_side
        prev_tp = self.tp_price
        prev_sl = self.sl_price
        prev_entry_price = self.entry_price
        prev_entry_time = self.entry_time
        prev_entry_qty = self.entry_qty
        prev_entry_event_id = self.entry_event_id
        prev_entry_signal_id = self.entry_signal_id

        self.has_position = False
        self.position_side = ""
        self.trade_pending = False
        self.tp_confirmed = False
        self.sl_confirmed = False
        self.tp_price = 0.0
        self.sl_price = 0.0
        self.entry_price = 0.0
        self.entry_time = 0
        self.entry_qty = 0.0
        self.entry_event_id = ""
        self.entry_signal_id = None
        self.pending_order = None

        try:
            from app.modules.binance_client import _load_algo_ids, _save_algo_ids
            data = _load_algo_ids()
            if self.symbol in data:
                data[self.symbol] = []
                _save_algo_ids(data)
        except Exception:
            pass

    def on_position_opened(self, side: str) -> None:
        self.has_position = True
        self.position_side = side
        if not self.pending_order:
            self.trade_pending = False

    def on_trade_pending(self) -> None:
        self.trade_pending = True

    async def try_execute_signal(self, signal: dict) -> bool:
        if self.has_position:
            return False
        if self.trade_pending:
            return False
        a_time = signal.get("candle_a_time")
        if a_time:
            self.used_a.add(a_time)
            self._save_used_a()
        return True

    # ── check_position_closed (polling) ─────────────────
    async def check_position_closed(self) -> None:
        async with self._state_lock:
            if not self.has_position:
                return
            try:
                positions = await get_position_risk(self.symbol)
                pos_amt = 0.0
                for p in positions:
                    if p.get("symbol") == self.symbol:
                        pos_amt = float(p.get("positionAmt", 0))
                        break
                if pos_amt != 0:
                    return
                from app.modules.binance_client import cancel_all_open_orders
                try:
                    await cancel_all_open_orders(self.symbol)
                except Exception:
                    pass
                self.on_position_closed()
                await log.ainfo("ha_position_closed_detected", symbol=self.symbol)
            except Exception as e:
                await log.awarning("ha_check_position_closed_error", symbol=self.symbol, error=str(e))

    # ── check_pending_fill ──────────────────────────────
    async def check_pending_fill(self) -> None:
        if not self.trade_pending or not self.pending_order:
            return
        async with self._state_lock:
            if not self.trade_pending or not self.pending_order:
                return
            await self._check_pending_fill_inner()

    async def _check_pending_fill_inner(self) -> None:
        po = self.pending_order

        # Timeout
        elapsed = time.time() - po["start_time"]
        if elapsed > po["timeout"]:
            try:
                from app.modules.binance_client import cancel_all_open_orders
                await cancel_all_open_orders(self.symbol)
            except Exception:
                pass
            self.trade_pending = False
            self.pending_order = None
            await log.ainfo("ha_pending_timeout", symbol=self.symbol, elapsed=int(elapsed))
            return

        # Limit order hala aktif mi?
        try:
            from app.modules.binance_client import get_order_status, cancel_all_open_orders as _cancel_all
            order_info = await get_order_status(self.symbol, int(po["order_id"]))
            order_status = order_info.get("status", "")
            if order_status in ("CANCELED", "EXPIRED", "REJECTED"):
                await log.ainfo("ha_pending_entry_cancelled", symbol=self.symbol, status=order_status)
                try:
                    await _cancel_all(self.symbol)
                except Exception:
                    pass
                self.trade_pending = False
                self.pending_order = None
                return
        except Exception:
            pass

        # Pozisyon kontrol
        try:
            from app.modules.binance_client import get_position_risk as _gpr
            from app.modules.binance_client import (
                round_price, place_take_profit_market_order,
                place_stop_market_order, place_stop_market_instant,
                place_market_order, cancel_all_open_orders,
            )
            from app.modules.indicator_settings_store import get_settings_or_defaults

            positions = await _gpr(self.symbol)
            pos_amt = 0.0
            entry_price = 0.0
            for p in positions:
                if p.get("symbol") == self.symbol:
                    pos_amt = float(p.get("positionAmt", 0))
                    entry_price = float(p.get("entryPrice", 0))
                    break

            if pos_amt == 0:
                return  # henuz dolmamis

            # FILL OLMUS
            qty = abs(pos_amt)
            side = po["side"]
            is_long = pos_amt > 0
            tick_size = po["tick_size"]
            sl_enabled = po["sl_enabled"]

            await log.ainfo("ha_pending_fill_detected", symbol=self.symbol, side=side, entry_price=entry_price, qty=qty)

            self.has_position = True
            self.position_side = "LONG" if is_long else "SHORT"
            self.trade_pending = False
            self.entry_price = entry_price
            self.entry_time = int(time.time())
            self.entry_qty = qty
            self.entry_event_id = po.get("event_id", "")
            self.entry_signal_id = po.get("signal_id")
            self.pending_order = None

            # TP/SL hesapla
            sym_cfg = await get_settings_or_defaults(self.symbol)
            tp_pct = sym_cfg.get("tp_pct", 1.3) / 100.0
            sl_pct = sym_cfg.get("sl_pct", 1.0) / 100.0

            if is_long:
                tp_price = round_price(entry_price * (1 + tp_pct), tick_size)
                sl_price = round_price(entry_price * (1 - sl_pct), tick_size)
                exit_side = "SELL"
            else:
                tp_price = round_price(entry_price * (1 - tp_pct), tick_size)
                sl_price = round_price(entry_price * (1 + sl_pct), tick_size)
                exit_side = "BUY"

            # Eski emirleri temizle
            try:
                await cancel_all_open_orders(self.symbol)
            except Exception:
                pass

            # SL koy
            if sl_enabled:
                try:
                    await place_stop_market_instant(self.symbol, exit_side, qty, sl_price)
                    self.sl_confirmed = True
                    self.sl_price = sl_price
                except Exception as e:
                    self.sl_confirmed = False
                    await log.aerror("ha_sl_place_failed", symbol=self.symbol, error=str(e))

            # TP koy
            try:
                await place_take_profit_market_order(self.symbol, exit_side, qty, tp_price)
                self.tp_confirmed = True
                self.tp_price = tp_price
            except Exception as e:
                await log.aerror("ha_tp_place_failed", symbol=self.symbol, error=str(e))

            # Yedek SL
            if not self.sl_confirmed and sl_enabled:
                try:
                    await place_stop_market_order(self.symbol, exit_side, qty, sl_price)
                    self.sl_confirmed = True
                    self.sl_price = sl_price
                except Exception as e:
                    await log.aerror("ha_sl_backup_failed", symbol=self.symbol, error=str(e))
                    # Emergency close
                    try:
                        await cancel_all_open_orders(self.symbol)
                        await place_market_order(self.symbol, exit_side, qty, reduce_only=True)
                        self.on_position_closed()
                        await log.ainfo("ha_sl_failed_emergency_close", symbol=self.symbol)
                    except Exception:
                        pass
                    return

            # Trade log
            from app.modules.trade_store import log_trade
            await log_trade(
                event_id=po.get("event_id", ""),
                ts=int(time.time()),
                symbol=self.symbol,
                side=side,
                quantity=qty,
                entry_price=entry_price,
                stop_price=sl_price if sl_enabled else None,
                order_id=po.get("order_id", ""),
                status="FILLED",
                signal_id=po.get("signal_id"),
            )

        except Exception as e:
            await log.aerror("ha_check_pending_fill_error", symbol=self.symbol, error=str(e))

    # ── Ana tick handler ────────────────────────────────
    async def on_price_tick(self, price: float) -> dict | None:
        if not self.warmed_up:
            return None

        now = int(time.time())

        # 1. Normal canli mum guncelle
        self.candle_high = max(self.candle_high, price)
        self.candle_low = min(self.candle_low, price)
        self.candle_close = price

        # 2. HA canli mum guncelle
        self._update_ha_candle()

        # 3. Mum kapandi mi?
        new_candle_start = (now // self.iv_sec) * self.iv_sec
        if new_candle_start > self.candle_start and now - new_candle_start > 2:
            # HA RSI hesapla
            closed_ha_rsi = self._calc_live_rsi(self.ha_candle_close)
            self._advance_candle(self.ha_candle_close)

            # HA state guncelle
            self.ha_prev_open = self.ha_candle_open
            self.ha_prev_close = self.ha_candle_close

            # Kapanan HA mumu kaydet
            closed = {
                "time": self.candle_start,
                "open": self.ha_candle_open,
                "high": self.ha_candle_high,
                "low": self.ha_candle_low,
                "close": self.ha_candle_close,
                "real_high": self.candle_high,
                "real_low": self.candle_low,
                "real_close": self.candle_close,
                "rsi": closed_ha_rsi,
            }
            self.closed_candles.append(closed)
            max_keep = max(self.max_gap + 20, 100)
            if len(self.closed_candles) > max_keep:
                self.closed_candles = self.closed_candles[-max_keep:]

            await log.ainfo(
                "ha_candle_closed",
                symbol=self.symbol,
                ha_close=round(self.ha_candle_close, 4),
                real_close=round(self.candle_close, 4),
                ha_high=round(self.ha_candle_high, 4),
                ha_low=round(self.ha_candle_low, 4),
                rsi=round(closed_ha_rsi, 2) if closed_ha_rsi else None,
            )

            # Yeni mum baslat
            self.candle_start = new_candle_start
            self.candle_open = price
            self.candle_high = price
            self.candle_low = price
            self.candle_close = price
            self._update_ha_candle()
            self.signal_fired_this_bar = False

        # 4. Bu mumda sinyal verildi mi?
        if self.signal_fired_this_bar:
            return None

        # 5. HA RSI hesapla
        live_ha_rsi = self._calc_live_rsi(self.ha_candle_close)
        if live_ha_rsi is None:
            return None

        # 6. Yeterli mum var mi?
        if len(self.closed_candles) < 1:
            return None

        # 7. Diverjans kontrol (HA mumlari uzerinden)
        signal = self._check_divergence(price, live_ha_rsi)
        if signal:
            self.signal_fired_this_bar = True
            self.last_signal_time = time.time()
            self.last_signal = signal
            self.last_signal_bar = self.candle_start
            return signal

        return None

    # ── Divergence tespiti (HA) ─────────────────────────
    def _check_divergence(self, current_price: float, live_ha_rsi: float) -> dict | None:
        """Hidden RSI Divergence — HA mumlari uzerinden."""
        n = len(self.closed_candles)
        search_range = min(self.max_gap, n)

        allowed = self.settings.get("allowed_directions", "BOTH")

        # SHORT (Bearish Hidden Divergence)
        # HA high karsilastirmasi, gercek high ile entry
        if allowed in ("BOTH", "SELL"):
            for gap in range(1, search_range + 1):
                a = self.closed_candles[n - gap]
                if a["rsi"] is None:
                    continue
                if a["time"] in self.used_a:
                    continue
                if (a["rsi"] >= self.short_thresh
                        and self.ha_candle_high > a["high"]
                        and live_ha_rsi < a["rsi"]):
                    # Entry price: GERCEK high uzerinden (HA degil)
                    entry_price = round(self.candle_high * (1 - self.entry_buffer), 6)
                    return {
                        "symbol": self.symbol,
                        "direction": "SELL",
                        "entry_price": entry_price,
                        "rsi_a": a["rsi"],
                        "rsi_b": live_ha_rsi,
                        "gap": gap,
                        "candle_a_time": a["time"],
                        "source": "ha_server",
                    }

        # LONG (Bullish Hidden Divergence)
        if allowed in ("BOTH", "BUY"):
            for gap in range(1, search_range + 1):
                a = self.closed_candles[n - gap]
                if a["rsi"] is None:
                    continue
                if a["time"] in self.used_a:
                    continue
                if (a["rsi"] <= self.long_thresh
                        and self.ha_candle_low < a["low"]
                        and live_ha_rsi > a["rsi"]):
                    # Entry price: GERCEK low uzerinden
                    entry_price = round(self.candle_low * (1 + self.entry_buffer), 6)
                    return {
                        "symbol": self.symbol,
                        "direction": "BUY",
                        "entry_price": entry_price,
                        "rsi_a": a["rsi"],
                        "rsi_b": live_ha_rsi,
                        "gap": gap,
                        "candle_a_time": a["time"],
                        "source": "ha_server",
                    }

        return None

    # ── Used-A persistence ──────────────────────────────
    def _used_a_path(self) -> Path:
        return Path(_DATA_DIR) / f"used_a_ha_{self.symbol}_{self.interval}.json"

    def _load_used_a(self) -> None:
        try:
            p = self._used_a_path()
            if p.exists():
                data = json.loads(p.read_text())
                self.used_a = set(data[-500:])
        except Exception:
            self.used_a = set()
        self._cleanup_used_a()

    def _cleanup_used_a(self) -> None:
        cutoff = int(time.time()) - 86400
        before = len(self.used_a)
        self.used_a = {ts for ts in self.used_a if ts > cutoff}
        if before != len(self.used_a):
            self._save_used_a()

    def _save_used_a(self) -> None:
        try:
            p = self._used_a_path()
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(sorted(self.used_a)[-500:]))
        except Exception:
            pass


# ══════════════════════════════════════════════════════════
# Engine lifecycle + loop
# ══════════════════════════════════════════════════════════


def get_ha_engine(symbol: str) -> HeikinAshiEngine | None:
    return _ha_engines.get(symbol.upper())


def get_all_ha_engines() -> dict[str, HeikinAshiEngine]:
    return _ha_engines


async def _ha_engine_loop() -> None:
    """HA engine ana dongusu — normal engine_loop ile ayni yapida."""
    from datetime import datetime, timezone, timedelta
    from app.modules.st_signal_logger import log_st_signal
    from app.modules.trade_executor import execute_trade

    tz_ist = timezone(timedelta(hours=3))

    # Motorlari baslat
    from app.modules.indicator_settings_store import get_all_settings
    try:
        all_settings = await get_all_settings()
        for s in all_settings:
            if s.get("active") and s.get("ha_enabled"):
                sym = s["symbol"]
                engine = HeikinAshiEngine(sym, s)
                await engine.warmup()
                _ha_engines[sym] = engine
                await log.ainfo("ha_engine_started", symbol=sym)
    except Exception as e:
        await log.aerror("ha_engine_init_error", error=str(e))

    if not _ha_engines:
        await log.ainfo("ha_engine_no_symbols")
        return

    last_fill_check = time.time()
    last_pos_sync = time.time()
    last_settings_reload = time.time()

    try:
        while True:
            await asyncio.sleep(0.2)
            now = time.time()

            # Fill takip + pozisyon kapanma (2sn)
            if now - last_fill_check > 2:
                last_fill_check = now
                for engine in _ha_engines.values():
                    if engine.trade_pending:
                        await engine.check_pending_fill()
                    elif engine.has_position:
                        await engine.check_position_closed()

            # Pozisyon sync (30sn)
            if now - last_pos_sync > 30:
                last_pos_sync = now
                for engine in _ha_engines.values():
                    await engine._sync_position()

            # Settings reload (60sn)
            if now - last_settings_reload > 60:
                last_settings_reload = now
                try:
                    fresh = await get_all_settings()
                    for s in fresh:
                        sym = s["symbol"]
                        if sym in _ha_engines:
                            eng = _ha_engines[sym]
                            eng.settings = s
                            eng.rsi_len = s.get("rsi_len", 10)
                            eng.long_thresh = s.get("long_thresh", 32.0)
                            eng.short_thresh = s.get("short_thresh", 70.0)
                            eng.max_gap = s.get("max_gap", 21)
                            eng.entry_buffer = s.get("entry_buffer", 0.1) / 100.0
                    for eng in _ha_engines.values():
                        eng._cleanup_used_a()
                except Exception as e:
                    await log.awarning("ha_settings_reload_error", error=str(e))

            # Her engine icin fiyat tick
            for sym, engine in _ha_engines.items():
                price = get_live_price(sym)
                if price is None:
                    continue

                signal = await engine.on_price_tick(price)
                if signal:
                    dt_str = datetime.now(tz_ist).strftime("%Y-%m-%d %H:%M:%S")

                    async with engine._state_lock:
                        should_trade = await engine.try_execute_signal(signal)

                    await log.ainfo(
                        "ha_signal",
                        symbol=sym,
                        direction=signal["direction"],
                        entry_price=signal["entry_price"],
                        rsi_a=signal["rsi_a"],
                        rsi_b=signal["rsi_b"],
                        gap=signal["gap"],
                        traded=should_trade,
                    )

                    skip_reason = None
                    if not should_trade:
                        if engine.has_position:
                            skip_reason = "pozisyon_acik"
                        elif engine.trade_pending:
                            skip_reason = "emir_bekliyor"

                    row_id = await log_st_signal(
                        dt=dt_str,
                        symbol=sym,
                        direction=signal["direction"],
                        band=engine.interval,
                        price=signal["entry_price"],
                        entered=should_trade,
                        source="ha_server",
                        rsi_a=signal["rsi_a"],
                        rsi_b=signal["rsi_b"],
                        gap=signal["gap"],
                        candle_a_time=signal.get("candle_a_time"),
                        skip_reason=skip_reason,
                    )

                    if should_trade:
                        from app.config import settings as app_settings
                        if app_settings.trading_enabled:
                            engine.on_trade_pending()
                            event_id = f"ha-{row_id}-{int(time.time())}"
                            asyncio.create_task(execute_trade(
                                symbol=sym,
                                signal=signal["direction"],
                                price=signal["entry_price"],
                                event_id=event_id,
                                tf=engine.interval,
                            ))

    except asyncio.CancelledError:
        await log.ainfo("ha_engine_loop_stopped")


def start_ha_engine_loop() -> None:
    global _ha_engine_task
    _ha_engine_task = asyncio.create_task(_ha_engine_loop())


async def stop_ha_engine_loop() -> None:
    global _ha_engine_task
    if _ha_engine_task:
        _ha_engine_task.cancel()
        try:
            await _ha_engine_task
        except asyncio.CancelledError:
            pass
        _ha_engine_task = None
