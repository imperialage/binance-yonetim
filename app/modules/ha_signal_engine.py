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
        # HA Reversal state
        self.prev_bull_signal: bool = False  # onceki mumda bullSignal (haOpen==haLow)
        self.prev_bear_signal: bool = False  # onceki mumda bearSignal (haOpen==haHigh)
        # RSI yön filtresi state
        self.prev_closed_rsi: float | None = None  # onceki mumun HA RSI'ı
        self.entry_bar_time: int = 0  # giris mumunun zamani (giris mumunda cikis engeli)

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

            async with httpx.AsyncClient(timeout=30, proxy=proxy_url) as client:
                resp = await client.get(BINANCE_URL, params={
                    "symbol": self.symbol, "interval": self.interval, "limit": 1500,
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

            # RSI state (HA close uzerinden — exit kontrolu icin aktif)
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

            # prev_closed_rsi — onceki mumun HA RSI'ı (yon karsilastirmasi icin)
            if len(self.closed_candles) >= 2:
                self.prev_closed_rsi = self.closed_candles[-2].get("rsi")

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
            # _sync_position ÇAĞIRMA — HA motor kendi _account_state
            # yönetimini yapıyor, SignalEngine.has_position kullanmıyor
            self.warmed_up = True
            await log.ainfo("ha_warmup_done", symbol=self.symbol, candles=len(self.closed_candles))

        except Exception as e:
            await log.aerror("ha_warmup_error", symbol=self.symbol, error=str(e))

    # ── Override: HA motor kendi _account_state yönetimini yapar ──
    # SignalEngine'den miras gelen bu metodlar HA'da NO-OP
    async def _sync_position(self) -> None:
        pass

    def on_position_closed(self, exit_info: dict | None = None) -> None:
        pass

    def on_position_opened(self, side: str) -> None:
        pass

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

    # ── Override: on_price_tick — HA Reversal + RSI Exit ──
    async def on_price_tick(self, price: float) -> dict | None:
        """HA Reversal + RSI Exit mantigi:
        - Giris: onceki mumda haOpen==haLow (LONG) veya haOpen==haHigh (SHORT)
        - Cikis: RSI exit (LONG: RSI>=exit_long, SHORT: RSI<=exit_short)
        - Ters sinyal: ters HA reversal gelirse kapat + yeni ac
        """
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
            # Binance'tan gercek OHLC cek
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

            # HA RSI — exit kontrolu icin kullaniliyor
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

            # ── HA Reversal sinyal tespiti (kapanan mum icin) ──
            tol = ha_l * 0.0001 if ha_l > 0 else 0.0001
            self.prev_bull_signal = abs(ha_o - ha_l) <= tol
            self.prev_bear_signal = abs(ha_o - ha_h) <= tol

            # ── RSI yön tespiti ──
            rsi_up = False
            rsi_down = False
            if closed_rsi is not None and self.prev_closed_rsi is not None:
                rsi_up = closed_rsi > self.prev_closed_rsi
                rsi_down = closed_rsi < self.prev_closed_rsi

            await log.ainfo("ha_candle_closed", symbol=self.symbol,
                            ha_o=round(ha_o, 4), ha_h=round(ha_h, 4),
                            ha_l=round(ha_l, 4), ha_c=round(ha_c, 4),
                            real_o=round(real_o, 4), real_c=round(real_c, 4),
                            rsi=round(closed_rsi, 2) if closed_rsi else None,
                            prev_rsi=round(self.prev_closed_rsi, 2) if self.prev_closed_rsi else None,
                            rsi_up=rsi_up, rsi_down=rsi_down,
                            tol=round(tol, 6),
                            bull=self.prev_bull_signal, bear=self.prev_bear_signal)

            # ── RSI Yön Exit — mum kapanışında kontrol ──
            if closed_rsi is not None and self.prev_closed_rsi is not None:
                pos = _get_pos(self.symbol)
                if pos["side"] is not None and pos.get("entry_bar") != self.candle_start:
                    if pos["side"] == "LONG" and rsi_down:
                        await log.ainfo("ha_rsi_direction_exit", symbol=self.symbol,
                                        side="LONG", rsi=round(closed_rsi, 2),
                                        prev_rsi=round(self.prev_closed_rsi, 2))
                        await _close_position(self.symbol, "RSI_DOWN")
                    elif pos["side"] == "SHORT" and rsi_up:
                        await log.ainfo("ha_rsi_direction_exit", symbol=self.symbol,
                                        side="SHORT", rsi=round(closed_rsi, 2),
                                        prev_rsi=round(self.prev_closed_rsi, 2))
                        await _close_position(self.symbol, "RSI_UP")

            # prev_closed_rsi güncelle (sonraki mum için)
            self.prev_closed_rsi = closed_rsi

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

        # 5. Sinyal: bullSignal[1] AND rsiUp / bearSignal[1] AND rsiDown
        # RSI yön filtresi: düşerken LONG açma, yükselirken SHORT açma
        if not self.prev_bull_signal and not self.prev_bear_signal:
            return None

        # RSI yön kontrolü için canlı RSI hesapla
        live_rsi = self._calc_live_rsi(self.ha_candle_close)
        if live_rsi is None or self.prev_closed_rsi is None:
            return None
        live_rsi_up = live_rsi > self.prev_closed_rsi
        live_rsi_down = live_rsi < self.prev_closed_rsi

        pos = _get_pos(self.symbol)

        if self.prev_bull_signal and live_rsi_up:
            if pos["side"] == "LONG":
                return None  # zaten LONG
            self.signal_fired_this_bar = True
            self.last_signal_time = time.time()
            signal = {
                "symbol": self.symbol, "direction": "BUY", "entry_price": price,
                "rsi_a": None, "rsi_b": None, "gap": None,
                "candle_a_time": self.candle_start, "source": "ha_server",
            }
            self.last_signal = signal
            return signal

        if self.prev_bear_signal and live_rsi_down:
            if pos["side"] == "SHORT":
                return None  # zaten SHORT
            self.signal_fired_this_bar = True
            self.last_signal_time = time.time()
            signal = {
                "symbol": self.symbol, "direction": "SELL", "entry_price": price,
                "rsi_a": None, "rsi_b": None, "gap": None,
                "candle_a_time": self.candle_start, "source": "ha_server",
            }
            self.last_signal = signal
            return signal

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


## Pozisyon state — sembol bazli tek hesap
# {symbol: {"side":str|None, "entry":float, "qty":float, "time":int, "entry_bar":int}}
_position_state: dict[str, dict] = {}


def _get_pos(sym: str) -> dict:
    if sym not in _position_state:
        _position_state[sym] = {"side": None, "entry": 0.0, "qty": 0.0, "time": 0, "entry_bar": 0}
    return _position_state[sym]


async def _close_position(sym: str, reason: str) -> None:
    """Pozisyonu market close ile kapat."""
    from app.modules.binance_client import (
        place_market_order, cancel_all_open_orders, get_position_risk,
    )
    pos = _get_pos(sym)
    if pos["side"] is None:
        return

    try:
        positions = await get_position_risk(sym)
        pos_amt = 0.0
        for p in positions:
            if p.get("symbol") == sym:
                pos_amt = float(p.get("positionAmt", 0))
                break

        if pos_amt == 0:
            pos["side"] = None
            pos["entry"] = 0.0
            pos["qty"] = 0.0
            return

        try:
            await cancel_all_open_orders(sym)
        except Exception:
            pass

        close_side = "SELL" if pos_amt > 0 else "BUY"
        qty = abs(pos_amt)
        await place_market_order(sym, close_side, qty, reduce_only=True)

        exit_price = get_live_price(sym) or 0
        entry_price = pos["entry"]
        pnl_pct = 0.0
        if entry_price > 0 and exit_price > 0:
            if pos["side"] == "LONG":
                pnl_pct = (exit_price - entry_price) / entry_price * 100
            elif pos["side"] == "SHORT":
                pnl_pct = (entry_price - exit_price) / entry_price * 100

        await log.ainfo("ha_position_closed", symbol=sym,
                        side=pos["side"], entry=pos["entry"], exit=round(exit_price, 6),
                        pnl_pct=round(pnl_pct, 3), reason=reason)

        try:
            from app.modules.st_signal_logger import log_st_signal
            from datetime import datetime, timezone, timedelta
            dt_str = datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")
            close_dir = "SELL" if pos["side"] == "LONG" else "BUY"
            await log_st_signal(
                dt=dt_str, symbol=sym, direction=f"CLOSE_{close_dir}",
                band="15M", price=exit_price,
                entered=True, source=f"ha_close_{reason.lower()}",
            )
        except Exception:
            pass

        pos["side"] = None
        pos["entry"] = 0.0
        pos["qty"] = 0.0
        pos["time"] = 0
        pos["entry_bar"] = 0

    except Exception as e:
        await log.aerror("ha_close_error", symbol=sym, error=str(e))


async def _open_position(sym: str, direction: str,
                         engine: "HeikinAshiEngine", row_id: int) -> None:
    """Market order ile pozisyon ac. Guvenlik SL opsiyonel."""
    from app.modules.binance_client import (
        get_total_wallet_balance, place_market_order, cancel_all_open_orders,
        place_stop_market_instant, round_price, round_step_size,
    )
    from app.modules.trade_executor import get_exchange_info_cached

    try:
        try:
            await cancel_all_open_orders(sym)
        except Exception:
            pass

        balance = await get_total_wallet_balance()
        price = get_live_price(sym)
        if not price or price <= 0:
            return

        info = await get_exchange_info_cached(sym)
        step_size = info["lotSize"]["stepSize"]
        min_qty = info["lotSize"]["minQty"]
        tick_size = info["priceFilter"]["tickSize"]

        sym_weight = engine.settings.get("weight", 0.10)
        usable = balance * sym_weight * 0.98
        raw_qty = usable / price
        quantity = round_step_size(raw_qty, step_size)

        if quantity < min_qty:
            await log.awarning("ha_qty_low", symbol=sym, qty=quantity, min=min_qty)
            return

        side = "BUY" if direction == "BUY" else "SELL"
        result = await place_market_order(sym, side, quantity)
        fill_price = float(result.get("avgPrice", 0)) or price

        pos = _get_pos(sym)
        pos["side"] = "LONG" if direction == "BUY" else "SHORT"
        pos["entry"] = fill_price
        pos["qty"] = quantity
        pos["time"] = int(time.time())
        pos["entry_bar"] = engine.candle_start

        await log.ainfo("ha_position_opened", symbol=sym,
                        direction=direction, entry=round(fill_price, 6), qty=quantity)

        sl_enabled = engine.settings.get("sl_enabled", True)
        if sl_enabled:
            sl_pct = engine.settings.get("sl_pct", 3.0) / 100.0
            if direction == "BUY":
                sl_price = round_price(fill_price * (1 - sl_pct), tick_size)
                sl_side = "SELL"
            else:
                sl_price = round_price(fill_price * (1 + sl_pct), tick_size)
                sl_side = "BUY"

            try:
                await place_stop_market_instant(sym, sl_side, quantity, sl_price)
                await log.ainfo("ha_safety_sl_placed", symbol=sym,
                                sl_price=sl_price, sl_side=sl_side)
            except Exception as e:
                await log.awarning("ha_safety_sl_failed", symbol=sym, error=str(e))

    except Exception as e:
        await log.aerror("ha_open_error", symbol=sym, error=str(e))


async def _ha_engine_loop() -> None:
    """HA Reversal + RSI Exit — 2 hesap ping-pong motoru.

    Her sembol icin:
    1. HA mum kapanisinda bullSignal/bearSignal tespit
    2. RSI exit: her iki hesabin pozisyonunu mum kapanisinda kontrol
    3. Sinyal geldiginde bos hesaptan ac, ters varsa kapat
    4. Guvenlik SL (opsiyonel, sl_enabled)
    """
    from datetime import datetime, timezone, timedelta
    from app.modules.st_signal_logger import log_st_signal
    tz_ist = timezone(timedelta(hours=3))

    # Baslangicta ha_enabled sembolleri yukle
    global _last_crash_error
    try:
        all_settings = await get_all_settings()
        ha_symbols = [s for s in all_settings if s.get("active") and s.get("ha_enabled")]
        await log.ainfo("ha_engine_loading", count=len(ha_symbols),
                        symbols=[s["symbol"] for s in ha_symbols])
        for s in ha_symbols:
            sym = s["symbol"]
            try:
                engine = HeikinAshiEngine(sym, s)
                await engine.warmup()
                if engine.warmed_up:
                    _ha_engines[sym] = engine
                    await log.ainfo("ha_engine_started", symbol=sym)
                else:
                    await log.awarning("ha_warmup_failed", symbol=sym)
            except Exception as we:
                _last_crash_error = f"warmup {sym}: {we}"
                await log.aerror("ha_warmup_exception", symbol=sym, error=str(we))
            await asyncio.sleep(3)  # Rate limit
    except Exception as e:
        _last_crash_error = f"init: {e}"
        await log.aerror("ha_engine_init_error", error=str(e))

    if not _ha_engines:
        await log.ainfo("ha_engine_no_symbols_yet")

    await log.ainfo("ha_engine_accounts", account_a=True)

    # ── STARTUP SYNC: Binance'tan pozisyonlari yukle ──
    from app.modules.binance_client import get_position_risk
    for sym in list(_ha_engines.keys()):
        try:
            pos = _get_pos(sym)
            positions = await get_position_risk(sym)
            for p in positions:
                if p.get("symbol") == sym:
                    amt = float(p.get("positionAmt", 0))
                    if amt > 0:
                        pos["side"] = "LONG"
                        pos["entry"] = float(p.get("entryPrice", 0))
                        pos["qty"] = abs(amt)
                    elif amt < 0:
                        pos["side"] = "SHORT"
                        pos["entry"] = float(p.get("entryPrice", 0))
                        pos["qty"] = abs(amt)
                    else:
                        pos["side"] = None
                    break
            await log.ainfo("ha_startup_sync", symbol=sym, side=pos["side"])
        except Exception as e:
            await log.awarning("ha_startup_sync_error", symbol=sym, error=str(e))
        await asyncio.sleep(1)

    last_pos_sync = time.time()
    last_settings_reload = time.time()

    try:
        while True:
            await asyncio.sleep(1)  # 1sn tick (rate limit dostu)
            now = time.time()

            # Pozisyon sync (300sn = 5dk) — Binance'tan gercek durumu al
            if now - last_pos_sync > 300:
                last_pos_sync = now
                for sym, engine in _ha_engines.items():
                    try:
                        from app.modules.binance_client import get_position_risk
                        positions = await get_position_risk(sym)
                        pos = _get_pos(sym)
                        for p in positions:
                            if p.get("symbol") == sym:
                                amt = float(p.get("positionAmt", 0))
                                if amt > 0:
                                    pos["side"] = "LONG"
                                    if pos["entry"] <= 0:
                                        pos["entry"] = float(p.get("entryPrice", 0))
                                        pos["qty"] = abs(amt)
                                elif amt < 0:
                                    pos["side"] = "SHORT"
                                    if pos["entry"] <= 0:
                                        pos["entry"] = float(p.get("entryPrice", 0))
                                        pos["qty"] = abs(amt)
                                else:
                                    pos["side"] = None
                                break
                    except Exception as e:
                        await log.awarning("ha_pos_sync_error", symbol=sym, error=str(e))

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
                            # RSI exit: yon karsilastirmasi (sabit threshold yok)
                        elif s.get("active") and s.get("ha_enabled") and sym not in _ha_engines:
                            new_eng = HeikinAshiEngine(sym, s)
                            await new_eng.warmup()
                            if new_eng.warmed_up:
                                _ha_engines[sym] = new_eng
                                await log.ainfo("ha_engine_new_symbol", symbol=sym)
                    ha_syms = {s["symbol"] for s in fresh if s.get("active") and s.get("ha_enabled")}
                    for sym in list(_ha_engines.keys()):
                        if sym not in ha_syms:
                            del _ha_engines[sym]
                            await log.ainfo("ha_engine_removed", symbol=sym)
                except Exception as e:
                    await log.awarning("ha_settings_reload_error", error=str(e))

            # ── Her engine icin fiyat tick ──
            for sym, engine in list(_ha_engines.items()):
                price = get_live_price(sym)
                if price is None:
                    continue

                signal = await engine.on_price_tick(price)

                # ── RSI Exit mum kapanisinda kontrol (on_price_tick icinde yapiliyor)
                # engine.on_price_tick mum kapanisinda RSI exit'i _bar_close_invalidate ile yapiyor
                # AMA bu sadece engine.has_position kontrol ediyor — ping-pong icin
                # her iki hesabi da kontrol etmemiz lazim. Bu on_price_tick SONRASI:

                # ── Sinyal varsa: ping-pong trade ──
                if signal:
                    dt_str = datetime.now(tz_ist).strftime("%Y-%m-%d %H:%M:%S")
                    from app.config import settings as app_settings

                    await log.ainfo("ha_signal", symbol=sym,
                                    direction=signal["direction"],
                                    entry_price=signal["entry_price"])

                    row_id = await log_st_signal(
                        dt=dt_str, symbol=sym, direction=signal["direction"],
                        band=engine.interval, price=signal["entry_price"],
                        entered=True, source="ha_server",
                        rsi_a=None, rsi_b=None, gap=None,
                        candle_a_time=signal.get("candle_a_time"),
                    )

                    if app_settings.trading_enabled:
                        # Pre-trade sync
                        try:
                            from app.modules.binance_client import get_position_risk
                            pos = _get_pos(sym)
                            positions = await get_position_risk(sym)
                            for p in positions:
                                if p.get("symbol") == sym:
                                    amt = float(p.get("positionAmt", 0))
                                    if amt > 0: pos["side"] = "LONG"; pos["qty"] = abs(amt)
                                    elif amt < 0: pos["side"] = "SHORT"; pos["qty"] = abs(amt)
                                    else: pos["side"] = None
                                    break
                        except Exception as _sync_err:
                            await log.awarning("ha_pre_trade_sync_error", symbol=sym, error=str(_sync_err))

                        pos = _get_pos(sym)
                        direction = signal["direction"]
                        new_side = "LONG" if direction == "BUY" else "SHORT"

                        # Ters pozisyon varsa kapat
                        opposite = "SHORT" if new_side == "LONG" else "LONG"
                        if pos["side"] == opposite:
                            await _close_position(sym, "REVERSE")

                        # Pozisyon ac (bos ise veya ters kapatildiysa)
                        pos = _get_pos(sym)
                        if pos["side"] is None:
                            await _open_position(sym, direction, engine, row_id)

    except asyncio.CancelledError:
        await log.ainfo("ha_engine_loop_stopped")


_last_crash_error: str = ""


async def _ha_engine_loop_safe() -> None:
    """Wrapper — exception'i loglar, task crash ettiginde gorunur."""
    global _last_crash_error
    try:
        await _ha_engine_loop()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        import traceback
        _last_crash_error = traceback.format_exc()
        await log.aerror("HA_ENGINE_LOOP_CRASHED", error=str(e), error_type=type(e).__name__)
        await log.aerror("HA_ENGINE_TRACEBACK", tb=_last_crash_error)


def start_ha_engine_loop() -> None:
    global _ha_engine_task
    _ha_engine_task = asyncio.create_task(_ha_engine_loop_safe())


async def stop_ha_engine_loop() -> None:
    global _ha_engine_task
    if _ha_engine_task:
        _ha_engine_task.cancel()
        try:
            await _ha_engine_task
        except asyncio.CancelledError:
            pass
        _ha_engine_task = None
