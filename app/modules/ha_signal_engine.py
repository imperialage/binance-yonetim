"""Heikin-Ashi Signal Engine — SignalEngine'den miras alir, sadece HA donusumu ekler.

Normal mumlar yerine Heikin-Ashi mumlari uzerinden RSI hesaplar.
Tum trade altyapisi (check_pending_fill, on_position_closed, TP/SL, order_stream)
SignalEngine'den aynen gelir — duplicate kod yok.

Override edilen 3 metod:
  1. warmup()        — klines → HA donusum → RSI
  2. on_price_tick() — tick → normal OHLC → HA OHLC → HA RSI → diverjans
  3. _check_divergence() — HA high/low ile kontrol, gercek fiyatla entry

HA Formulleri:
  HA_Close = (O + H + L + C) / 4
  HA_Open  = (prev_HA_Open + prev_HA_Close) / 2
  HA_High  = max(H, HA_Open, HA_Close)
  HA_Low   = min(L, HA_Open, HA_Close)
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from app.modules.signal_engine import SignalEngine, BINANCE_URL, _DATA_DIR, INTERVAL_SECONDS
from app.modules.rsi_calculator import calculate_rsi
from app.modules.price_stream import get_live_price
from app.modules.indicator_settings_store import get_all_settings
from app.utils.logging import get_logger

log = get_logger(__name__)

# HA engine registry (SignalEngine'in _engines'inden ayri)
_ha_engines: dict[str, HeikinAshiEngine] = {}
_ha_engine_task: asyncio.Task | None = None


# ── Heikin-Ashi donusum fonksiyonlari ─────────────────


def convert_klines_to_ha(klines: list) -> list[dict]:
    """Normal Binance klines → Heikin-Ashi mumlari."""
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
        })
        prev_ha_open = ha_open
        prev_ha_close = ha_close
    return result


def _calc_ha(o: float, h: float, l: float, c: float,
             prev_ha_open: float, prev_ha_close: float) -> tuple[float, float, float, float]:
    """Tek mum HA hesabi → (ha_open, ha_high, ha_low, ha_close)."""
    ha_close = (o + h + l + c) / 4
    ha_open = (prev_ha_open + prev_ha_close) / 2
    return ha_open, max(h, ha_open, ha_close), min(l, ha_open, ha_close), ha_close


# ── HeikinAshiEngine ──────────────────────────────────


class HeikinAshiEngine(SignalEngine):
    """SignalEngine'den miras — sadece HA donusumu ekler."""

    def __init__(self, symbol: str, settings: dict[str, Any]):
        super().__init__(symbol, settings)
        # HA state
        self.ha_prev_open: float = 0.0
        self.ha_prev_close: float = 0.0
        self.ha_candle_open: float = 0.0
        self.ha_candle_high: float = 0.0
        self.ha_candle_low: float = 0.0
        self.ha_candle_close: float = 0.0

    # ── Override: Warmup ────────────────────────────────
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
                    "symbol": self.symbol, "interval": self.interval, "limit": 200,
                })
                resp.raise_for_status()
                klines = resp.json()

            if not klines or len(klines) < self.rsi_len + 2:
                await log.awarning("ha_warmup_insufficient", symbol=self.symbol)
                return

            # Normal klines → HA
            ha_candles = convert_klines_to_ha(klines)
            ha_closes = [c["close"] for c in ha_candles]
            rsi_values = calculate_rsi(ha_closes, self.rsi_len)

            # Kapanmis HA mumlari (son mum haric)
            self.closed_candles = []
            for i in range(len(ha_candles) - 1):
                hc = ha_candles[i]
                hc["rsi"] = rsi_values[i] if i < len(rsi_values) else None
                self.closed_candles.append(hc)

            # RSI state (HA close uzerinden)
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

            # HA state
            if self.closed_candles:
                last_closed = self.closed_candles[-1]
                self.ha_prev_open = last_closed["open"]
                self.ha_prev_close = last_closed["close"]

            # Canli mum (normal OHLC)
            last = klines[-1]
            self.candle_start = int(last[0]) // 1000
            self.candle_open = float(last[1])
            self.candle_high = float(last[2])
            self.candle_low = float(last[3])
            self.candle_close = float(last[4])
            self._update_ha_candle()

            self._load_used_a()
            await self._sync_position()
            self.warmed_up = True
            await log.ainfo("ha_warmup_done", symbol=self.symbol, candles=len(self.closed_candles))

        except Exception as e:
            await log.aerror("ha_warmup_error", symbol=self.symbol, error=str(e))

    # ── HA mum guncelleme ───────────────────────────────
    def _update_ha_candle(self) -> None:
        ha_o, ha_h, ha_l, ha_c = _calc_ha(
            self.candle_open, self.candle_high, self.candle_low, self.candle_close,
            self.ha_prev_open, self.ha_prev_close,
        )
        self.ha_candle_open = ha_o
        self.ha_candle_high = ha_h
        self.ha_candle_low = ha_l
        self.ha_candle_close = ha_c

    # ── Override: on_price_tick ──────────────────────────
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
            # Binance'tan gercek OHLC cek (RSI drift fix)
            real_o, real_h, real_l, real_c = self.candle_open, self.candle_high, self.candle_low, self.candle_close
            try:
                import httpx
                proxy_url = None
                try:
                    from app.config import settings as _s
                    proxy_url = _s.binance_proxy_url or None
                except Exception:
                    pass
                async with httpx.AsyncClient(timeout=5, proxy=proxy_url) as _client:
                    _resp = await _client.get(BINANCE_URL, params={
                        "symbol": self.symbol, "interval": self.interval, "limit": 2,
                    })
                    if _resp.is_success:
                        _kl = _resp.json()
                        if _kl and len(_kl) >= 2:
                            real_o = float(_kl[0][1])
                            real_h = float(_kl[0][2])
                            real_l = float(_kl[0][3])
                            real_c = float(_kl[0][4])
            except Exception:
                pass

            # Gercek OHLC'den HA hesapla
            ha_o, ha_h, ha_l, ha_c = _calc_ha(real_o, real_h, real_l, real_c,
                                               self.ha_prev_open, self.ha_prev_close)

            # HA RSI
            closed_rsi = self._calc_live_rsi(ha_c)
            self._advance_candle(ha_c)

            # HA state guncelle
            self.ha_prev_open = ha_o
            self.ha_prev_close = ha_c

            # Kapanan mumu kaydet
            closed = {
                "time": self.candle_start,
                "open": ha_o, "high": ha_h, "low": ha_l, "close": ha_c,
                "real_open": real_o, "real_high": real_h, "real_low": real_l, "real_close": real_c,
                "rsi": closed_rsi,
            }
            self.closed_candles.append(closed)
            max_keep = max(self.max_gap + 20, 100)
            if len(self.closed_candles) > max_keep:
                self.closed_candles = self.closed_candles[-max_keep:]

            await log.ainfo("ha_candle_closed", symbol=self.symbol,
                            ha_close=round(ha_c, 4), real_close=round(real_c, 4),
                            rsi=round(closed_rsi, 2) if closed_rsi else None)

            # Pine Script uyumlu: kapanan mum B olarak tekrar test edilmiyor

            # Yeni mum
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

        # 5. Pozisyon kontrolu — Pine Script uyumlu: pozisyon varsa ARAMA
        if self.has_position or self.trade_pending:
            return None

        # 6. HA RSI
        live_rsi = self._calc_live_rsi(self.ha_candle_close)
        if live_rsi is None:
            return None

        if len(self.closed_candles) < 1:
            return None

        # 7. Diverjans (HA)
        signal = self._check_divergence(price, live_rsi)
        if signal:
            self.signal_fired_this_bar = True
            self.last_signal_time = time.time()
            self.last_signal = signal
            self.last_signal_bar = self.candle_start
            return signal

        return None

    # ── Override: _check_divergence (HA) ────────────────
    def _check_divergence(self, current_price: float, live_ha_rsi: float) -> dict | None:
        """Hidden RSI Divergence — HA mumlari uzerinden, gercek fiyatla entry."""
        n = len(self.closed_candles)
        search_range = min(self.max_gap, n)
        allowed = self.settings.get("allowed_directions", "BOTH")

        # SHORT
        if allowed in ("BOTH", "SELL"):
            for gap in range(1, search_range + 1):
                a = self.closed_candles[n - gap]
                if a["rsi"] is None or a["time"] in self.used_a:
                    continue
                if (a["rsi"] >= self.short_thresh
                        and self.ha_candle_high > a["high"]
                        and live_ha_rsi < a["rsi"]):
                    entry_price = round(self.candle_high * (1 - self.entry_buffer), 6)
                    return {
                        "symbol": self.symbol, "direction": "SELL", "entry_price": entry_price,
                        "rsi_a": a["rsi"], "rsi_b": live_ha_rsi, "gap": gap,
                        "candle_a_time": a["time"], "source": "ha_server",
                    }

        # LONG
        if allowed in ("BOTH", "BUY"):
            for gap in range(1, search_range + 1):
                a = self.closed_candles[n - gap]
                if a["rsi"] is None or a["time"] in self.used_a:
                    continue
                if (a["rsi"] <= self.long_thresh
                        and self.ha_candle_low < a["low"]
                        and live_ha_rsi > a["rsi"]):
                    entry_price = round(self.candle_low * (1 + self.entry_buffer), 6)
                    return {
                        "symbol": self.symbol, "direction": "BUY", "entry_price": entry_price,
                        "rsi_a": a["rsi"], "rsi_b": live_ha_rsi, "gap": gap,
                        "candle_a_time": a["time"], "source": "ha_server",
                    }
        return None

    # ── Override: used_a path (HA ayri dosya) ───────────
    def _used_a_path(self) -> Path:
        return Path(_DATA_DIR) / f"used_a_ha_{self.symbol}_{self.interval}.json"


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

    # Baslangicta ha_enabled sembolleri yukle
    try:
        all_settings = await get_all_settings()
        for s in all_settings:
            if s.get("active") and s.get("ha_enabled"):
                sym = s["symbol"]
                engine = HeikinAshiEngine(sym, s)
                await engine.warmup()
                if engine.warmed_up:
                    _ha_engines[sym] = engine
                    await log.ainfo("ha_engine_started", symbol=sym)
    except Exception as e:
        await log.aerror("ha_engine_init_error", error=str(e))

    if not _ha_engines:
        await log.ainfo("ha_engine_no_symbols_yet")

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
                        elif s.get("active") and s.get("ha_enabled") and sym not in _ha_engines:
                            new_eng = HeikinAshiEngine(sym, s)
                            await new_eng.warmup()
                            if new_eng.warmed_up:
                                _ha_engines[sym] = new_eng
                                await log.ainfo("ha_engine_new_symbol", symbol=sym)
                    # ha_enabled kapanan sembolleri kaldir
                    ha_syms = {s["symbol"] for s in fresh if s.get("active") and s.get("ha_enabled")}
                    for sym in list(_ha_engines.keys()):
                        if sym not in ha_syms:
                            del _ha_engines[sym]
                            await log.ainfo("ha_engine_removed", symbol=sym)
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

                    # A mumunu her sinyal uretiminde used_a'ya ekle (tekrar kullanilmasin)
                    a_time = signal.get("candle_a_time")
                    if a_time:
                        engine.used_a.add(a_time)
                        engine._save_used_a()

                    async with engine._state_lock:
                        should_trade = await engine.try_execute_signal(signal)

                    await log.ainfo("ha_signal", symbol=sym,
                                    direction=signal["direction"], entry_price=signal["entry_price"],
                                    rsi_a=signal["rsi_a"], rsi_b=signal["rsi_b"],
                                    gap=signal["gap"], traded=should_trade)

                    skip_reason = None
                    if not should_trade:
                        if engine.has_position:
                            skip_reason = "pozisyon_acik"
                        elif engine.trade_pending:
                            skip_reason = "emir_bekliyor"

                    row_id = await log_st_signal(
                        dt=dt_str, symbol=sym, direction=signal["direction"],
                        band=engine.interval, price=signal["entry_price"],
                        entered=should_trade, source="ha_server",
                        rsi_a=signal["rsi_a"], rsi_b=signal["rsi_b"],
                        gap=signal["gap"], candle_a_time=signal.get("candle_a_time"),
                        skip_reason=skip_reason,
                    )

                    if should_trade:
                        from app.config import settings as app_settings
                        if app_settings.trading_enabled:
                            from app.modules.binance_client import get_position_risk as _gpr, get_usdt_balance as _gub
                            _pf = asyncio.ensure_future(asyncio.gather(_gpr(sym), _gub()))
                            engine.on_trade_pending()
                            event_id = f"ha-{row_id}-{int(time.time())}"
                            asyncio.create_task(execute_trade(
                                symbol=sym, signal=signal["direction"],
                                price=signal["entry_price"], event_id=event_id,
                                tf=engine.interval, prefetch=_pf,
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
