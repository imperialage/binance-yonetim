"""Server-side signal engine — tick-bazli Hidden RSI Divergence sinyal uretici.

WebSocket fiyat tick'lerini dinler, anlik RSI + divergence kontrol eder.
Pozisyon durumunu Binance'tan takip eder.
Sinyal bulunca dogrudan trade_executor'a gonderir.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

from app.modules.rsi_calculator import calculate_rsi
from app.modules.indicator_settings_store import get_all_settings, get_settings_or_defaults
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

# Aktif engine'ler: key = "SYMBOL"
_engines: dict[str, SignalEngine] = {}
_engine_task: asyncio.Task | None = None


class SignalEngine:
    """Tek sembol icin tick-bazli sinyal motoru."""

    def __init__(self, symbol: str, settings: dict[str, Any]):
        self.symbol = symbol
        self.settings = settings
        self.interval = settings.get("interval", "15m")
        self.iv_sec = INTERVAL_SECONDS.get(self.interval, 900)

        # Indikator parametreleri
        self.rsi_len: int = settings.get("rsi_len", 10)
        self.long_thresh: float = settings.get("long_thresh", 32.0)
        self.short_thresh: float = settings.get("short_thresh", 70.0)
        self.max_gap: int = settings.get("max_gap", 12)
        self.entry_buffer: float = settings.get("entry_buffer", 0.1) / 100.0  # yuzde → oran

        # Kapanmis mumlar + RSI (warmup ile doldurulacak)
        self.closed_candles: list[dict] = []  # [{time, open, high, low, close, rsi}, ...]

        # Canli mum state
        self.candle_start: int = 0
        self.candle_open: float = 0.0
        self.candle_high: float = 0.0
        self.candle_low: float = float("inf")
        self.candle_close: float = 0.0

        # RSI state (Wilder's RMA)
        self.rsi_avg_gain: float = 0.0
        self.rsi_avg_loss: float = 0.0
        self.rsi_prev_close: float = 0.0
        self.rsi_warmed_up: bool = False

        # Pozisyon state
        self.has_position: bool = False
        self.position_side: str = ""  # "LONG" / "SHORT"

        # Sinyal state
        self.signal_fired_this_bar: bool = False
        self.trade_pending: bool = False  # execute_trade cagrildi, fill bekleniyor
        self.used_a: set[int] = set()  # kullanilmis A mum zamanlari
        self.warmed_up: bool = False
        self.last_signal_time: float = 0.0

    # ── Warmup ──────────────────────────────────────────
    async def warmup(self) -> None:
        """Binance'tan 200 mum cek, RSI hesapla, closed_candles doldur."""
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
                await log.awarning("signal_engine_warmup_insufficient", symbol=self.symbol, count=len(klines) if klines else 0)
                return

            # Close fiyatlarini cek
            closes = [float(k[4]) for k in klines]

            # RSI hesapla (tam seri)
            rsi_values = calculate_rsi(closes, self.rsi_len)

            # Kapanmis mumlari doldur (son mum haric — o canli)
            self.closed_candles = []
            for i in range(len(klines) - 1):
                k = klines[i]
                self.closed_candles.append({
                    "time": int(k[0]) // 1000,
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "rsi": rsi_values[i] if i < len(rsi_values) else None,
                })

            # RSI state — son kapanmis mumun state'i
            n = len(closes) - 1  # son mum haric
            if n > self.rsi_len:
                gains = [0.0] * n
                losses = [0.0] * n
                for i in range(1, n):
                    d = closes[i] - closes[i - 1]
                    gains[i] = max(d, 0.0)
                    losses[i] = max(-d, 0.0)

                self.rsi_avg_gain = sum(gains[1:self.rsi_len + 1]) / self.rsi_len
                self.rsi_avg_loss = sum(losses[1:self.rsi_len + 1]) / self.rsi_len

                for i in range(self.rsi_len + 1, n):
                    self.rsi_avg_gain = (self.rsi_avg_gain * (self.rsi_len - 1) + gains[i]) / self.rsi_len
                    self.rsi_avg_loss = (self.rsi_avg_loss * (self.rsi_len - 1) + losses[i]) / self.rsi_len

                self.rsi_prev_close = closes[n - 1]
                self.rsi_warmed_up = True

            # Canli mumu baslat
            last = klines[-1]
            self.candle_start = int(last[0]) // 1000
            self.candle_open = float(last[1])
            self.candle_high = float(last[2])
            self.candle_low = float(last[3])
            self.candle_close = float(last[4])

            # used_a yukle
            self._load_used_a()

            # Pozisyon kontrol
            await self._sync_position()

            self.warmed_up = True
            await log.ainfo(
                "signal_engine_warmup_done",
                symbol=self.symbol,
                interval=self.interval,
                candles=len(self.closed_candles),
                has_position=self.has_position,
            )

        except Exception as e:
            await log.aerror("signal_engine_warmup_error", symbol=self.symbol, error=str(e))

    # ── Anlık RSI hesaplama ──────────────────────────────
    def _calc_live_rsi(self, price: float) -> float | None:
        """Mevcut RMA state'ten anlik RSI hesapla (state degistirmez)."""
        if not self.rsi_warmed_up:
            return None
        d = price - self.rsi_prev_close
        g = max(d, 0.0)
        l_val = max(-d, 0.0)
        ag = (self.rsi_avg_gain * (self.rsi_len - 1) + g) / self.rsi_len
        al = (self.rsi_avg_loss * (self.rsi_len - 1) + l_val) / self.rsi_len
        if al == 0:
            return 100.0
        return round(100.0 - (100.0 / (1.0 + ag / al)), 2)

    def _advance_candle(self, closed_price: float) -> None:
        """Mum kapandi — RSI state ilerlet."""
        if not self.rsi_warmed_up:
            return
        d = closed_price - self.rsi_prev_close
        g = max(d, 0.0)
        l_val = max(-d, 0.0)
        self.rsi_avg_gain = (self.rsi_avg_gain * (self.rsi_len - 1) + g) / self.rsi_len
        self.rsi_avg_loss = (self.rsi_avg_loss * (self.rsi_len - 1) + l_val) / self.rsi_len
        self.rsi_prev_close = closed_price

    # ── Pozisyon senkronizasyonu ─────────────────────────
    async def _sync_position(self) -> None:
        """Binance'tan gercek pozisyon durumunu al. trade_pending'i de duzelt."""
        try:
            positions = await get_position_risk(self.symbol)
            for p in positions:
                if p.get("symbol") == self.symbol:
                    amt = float(p.get("positionAmt", 0))
                    if amt > 0:
                        self.has_position = True
                        self.position_side = "LONG"
                        self.trade_pending = False
                    elif amt < 0:
                        self.has_position = True
                        self.position_side = "SHORT"
                        self.trade_pending = False
                    else:
                        self.has_position = False
                        self.position_side = ""
                        self.trade_pending = False  # timeout/fail durumunu temizle
                    return
            self.has_position = False
            self.position_side = ""
            self.trade_pending = False
        except Exception as e:
            await log.awarning("signal_engine_position_check_error", symbol=self.symbol, error=str(e))

    # ── Pozisyon kapandi bildirimi (order_stream'den) ────
    def on_position_closed(self) -> None:
        """TP/SL fill → pozisyon kapandi."""
        self.has_position = False
        self.position_side = ""
        self.trade_pending = False

    def on_position_opened(self, side: str) -> None:
        """Limit order fill → pozisyon acildi."""
        self.has_position = True
        self.position_side = side
        self.trade_pending = False

    def on_trade_pending(self) -> None:
        """execute_trade cagrildi, fill bekleniyor."""
        self.trade_pending = True

    # ── Ana tick handler ─────────────────────────────────
    async def on_price_tick(self, price: float) -> dict | None:
        """Her fiyat tick'inde cagirilir. Sinyal bulursa dict dondurur."""
        if not self.warmed_up:
            return None

        now = int(time.time())

        # ── 1. Canli mum guncelle ──
        self.candle_high = max(self.candle_high, price)
        self.candle_low = min(self.candle_low, price)
        self.candle_close = price

        # ── 2. Mum kapandi mi? ──
        new_candle_start = (now // self.iv_sec) * self.iv_sec
        if new_candle_start > self.candle_start and now - new_candle_start > 2:
            # Kapanan mumun RSI'ini hesapla
            closed_rsi = self._calc_live_rsi(self.candle_close)
            self._advance_candle(self.candle_close)

            # Kapanan mumu kaydet
            closed = {
                "time": self.candle_start,
                "open": self.candle_open,
                "high": self.candle_high,
                "low": self.candle_low,
                "close": self.candle_close,
                "rsi": closed_rsi,
            }
            self.closed_candles.append(closed)
            # Son max_gap + 5 mum yeterli
            if len(self.closed_candles) > self.max_gap + 20:
                self.closed_candles = self.closed_candles[-(self.max_gap + 20):]

            # Yeni mum baslat
            self.candle_start = new_candle_start
            self.candle_open = price
            self.candle_high = price
            self.candle_low = price
            self.candle_close = price
            self.signal_fired_this_bar = False

        # ── 3. once_per_bar — bu mumda sinyal verildi mi? ──
        if self.signal_fired_this_bar:
            return None

        # ── 4. Pozisyon varsa veya emir bekliyorsa sinyal arama ──
        if self.has_position or self.trade_pending:
            return None

        # ── 5. Anlik RSI hesapla ──
        live_rsi = self._calc_live_rsi(price)
        if live_rsi is None:
            return None

        # ── 6. Yeterli kapanmis mum var mi? ──
        n = len(self.closed_candles)
        if n < 1:
            return None

        # ── 7. Hidden Divergence kontrolu ──
        signal = self._check_divergence(price, live_rsi)
        if signal:
            self.signal_fired_this_bar = True
            self.last_signal_time = time.time()
            self._save_used_a()
            return signal

        return None

    # ── Divergence tespiti ───────────────────────────────
    def _check_divergence(self, current_price: float, live_rsi: float) -> dict | None:
        """Hidden RSI Divergence kontrol — canli mum = B, gecmis mumlar = A adaylari."""
        n = len(self.closed_candles)
        search_range = min(self.max_gap, n)

        # Allowed directions
        allowed = self.settings.get("allowed_directions", "BOTH")

        # ── SHORT (Bearish Hidden Divergence) ──
        if allowed in ("BOTH", "SELL"):
            for gap in range(1, search_range + 1):
                a = self.closed_candles[n - gap]
                if a["rsi"] is None:
                    continue
                if a["time"] in self.used_a:
                    continue
                if (a["rsi"] >= self.short_thresh
                        and self.candle_high > a["high"]
                        and live_rsi < a["rsi"]):
                    entry_price = round(self.candle_high * (1 - self.entry_buffer), 6)
                    self.used_a.add(a["time"])
                    return {
                        "symbol": self.symbol,
                        "direction": "SELL",
                        "entry_price": entry_price,
                        "rsi_a": a["rsi"],
                        "rsi_b": live_rsi,
                        "gap": gap,
                        "candle_a_time": a["time"],
                        "source": "server",
                    }

        # ── LONG (Bullish Hidden Divergence) ──
        if allowed in ("BOTH", "BUY"):
            for gap in range(1, search_range + 1):
                a = self.closed_candles[n - gap]
                if a["rsi"] is None:
                    continue
                if a["time"] in self.used_a:
                    continue
                if (a["rsi"] <= self.long_thresh
                        and self.candle_low < a["low"]
                        and live_rsi > a["rsi"]):
                    entry_price = round(self.candle_low * (1 + self.entry_buffer), 6)
                    self.used_a.add(a["time"])
                    return {
                        "symbol": self.symbol,
                        "direction": "BUY",
                        "entry_price": entry_price,
                        "rsi_a": a["rsi"],
                        "rsi_b": live_rsi,
                        "gap": gap,
                        "candle_a_time": a["time"],
                        "source": "server",
                    }

        return None

    # ── Used-A persistence ───────────────────────────────
    def _used_a_path(self) -> Path:
        return Path(_DATA_DIR) / f"used_a_{self.symbol}_{self.interval}.json"

    def _load_used_a(self) -> None:
        try:
            p = self._used_a_path()
            if p.exists():
                data = json.loads(p.read_text())
                self.used_a = set(data[-500:])
        except Exception:
            self.used_a = set()

    def _save_used_a(self) -> None:
        try:
            p = self._used_a_path()
            p.parent.mkdir(parents=True, exist_ok=True)
            trimmed = sorted(self.used_a)[-500:]
            p.write_text(json.dumps(trimmed))
        except Exception:
            pass


# ══════════════════════════════════════════════════════════
# Engine lifecycle
# ══════════════════════════════════════════════════════════

async def _engine_loop() -> None:
    """Ana dongu — tum aktif engine'leri fiyat tick'leriyle besler."""
    from app.modules.trade_executor import execute_trade
    from app.modules.st_signal_logger import log_st_signal
    from datetime import datetime, timezone, timedelta

    tz_ist = timezone(timedelta(hours=3))

    # Warmup bekle — price_stream hazir olsun
    await asyncio.sleep(5)

    # Engine'leri olustur
    try:
        all_settings = await get_all_settings()
    except Exception as e:
        await log.aerror("signal_engine_settings_error", error=str(e))
        return

    active_symbols = [s for s in all_settings if s.get("active") and s.get("listening")]
    if not active_symbols:
        await log.ainfo("signal_engine_no_active_symbols")
        return

    for s in active_symbols:
        sym = s["symbol"]
        engine = SignalEngine(sym, s)
        await engine.warmup()
        if not engine.warmed_up:
            # Retry 3 kez, 10sn arayla
            for attempt in range(1, 4):
                await log.awarning("signal_engine_warmup_retry", symbol=sym, attempt=attempt)
                await asyncio.sleep(10)
                await engine.warmup()
                if engine.warmed_up:
                    break
        if engine.warmed_up:
            _engines[sym] = engine
        else:
            await log.aerror("signal_engine_warmup_failed", symbol=sym)

    await log.ainfo("signal_engine_started", symbols=list(_engines.keys()))

    # Pozisyon sync periyodik (30sn)
    last_pos_sync = time.time()
    POS_SYNC_INTERVAL = 30

    # Settings reload periyodik (60sn)
    last_settings_reload = time.time()
    SETTINGS_RELOAD_INTERVAL = 60

    # Ana dongu — her 200ms fiyat kontrol
    while True:
        try:
            await asyncio.sleep(0.2)

            now = time.time()

            # Periyodik pozisyon sync (30sn)
            if now - last_pos_sync > POS_SYNC_INTERVAL:
                last_pos_sync = now
                for engine in _engines.values():
                    await engine._sync_position()

            # Periyodik settings reload (60sn)
            if now - last_settings_reload > SETTINGS_RELOAD_INTERVAL:
                last_settings_reload = now
                try:
                    fresh = await get_all_settings()
                    for s in fresh:
                        sym = s["symbol"]
                        if sym in _engines:
                            eng = _engines[sym]
                            eng.settings = s
                            eng.rsi_len = s.get("rsi_len", 10)
                            eng.long_thresh = s.get("long_thresh", 32.0)
                            eng.short_thresh = s.get("short_thresh", 70.0)
                            eng.max_gap = s.get("max_gap", 12)
                            eng.entry_buffer = s.get("entry_buffer", 0.1) / 100.0
                        elif s.get("active") and s.get("listening"):
                            # Yeni sembol eklendi — engine olustur
                            new_engine = SignalEngine(sym, s)
                            await new_engine.warmup()
                            _engines[sym] = new_engine
                            await log.ainfo("signal_engine_new_symbol", symbol=sym)
                    # Silinen/deaktif sembolleri kaldir
                    active_syms = {s["symbol"] for s in fresh if s.get("active") and s.get("listening")}
                    for sym in list(_engines.keys()):
                        if sym not in active_syms:
                            del _engines[sym]
                            await log.ainfo("signal_engine_removed", symbol=sym)
                except Exception as e:
                    await log.awarning("signal_engine_settings_reload_error", error=str(e))

            # Her engine icin fiyat tick
            for sym, engine in _engines.items():
                price = get_live_price(sym)
                if price is None:
                    continue

                signal = await engine.on_price_tick(price)
                if signal:
                    # Sinyal bulundu!
                    dt_str = datetime.now(tz_ist).strftime("%Y-%m-%d %H:%M:%S")

                    await log.ainfo(
                        "signal_engine_signal",
                        symbol=sym,
                        direction=signal["direction"],
                        entry_price=signal["entry_price"],
                        rsi_a=signal["rsi_a"],
                        rsi_b=signal["rsi_b"],
                        gap=signal["gap"],
                    )

                    # Signal log
                    row_id = await log_st_signal(
                        dt=dt_str,
                        symbol=sym,
                        direction=signal["direction"],
                        band=engine.interval,
                        price=signal["entry_price"],
                        entered=True,
                    )

                    # Trade execute
                    # NOT: on_position_opened burada CAGRILMAZ.
                    # Pozisyon acilmasini order_stream callback'inden ogreniyoruz.
                    # Boylece execute_trade basarisiz olursa state yanlis kalmaz.
                    from app.config import settings as app_settings
                    if app_settings.trading_enabled:
                        engine.on_trade_pending()
                        event_id = f"se-{row_id}-{int(time.time())}"
                        asyncio.create_task(execute_trade(
                            symbol=sym,
                            signal=signal["direction"],
                            price=signal["entry_price"],
                            event_id=event_id,
                            tf=engine.interval,
                        ))

        except asyncio.CancelledError:
            break
        except Exception as e:
            await log.aerror("signal_engine_loop_error", error=str(e))
            await asyncio.sleep(2)


def start_signal_engines() -> None:
    """Signal engine'leri baslat (main.py lifespan'dan cagirilir)."""
    global _engine_task
    _engine_task = asyncio.create_task(_engine_loop())


async def stop_signal_engines() -> None:
    """Signal engine'leri durdur."""
    global _engine_task
    if _engine_task:
        _engine_task.cancel()
        try:
            await _engine_task
        except asyncio.CancelledError:
            pass
        _engine_task = None
    _engines.clear()


def get_engine(symbol: str) -> SignalEngine | None:
    """Belirli sembolun engine'ini dondur (monitor icin)."""
    return _engines.get(symbol.upper())


def get_all_engines() -> dict[str, SignalEngine]:
    """Tum engine'leri dondur."""
    return _engines
