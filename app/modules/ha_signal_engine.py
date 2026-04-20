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
        # RSI exit esikleri (settings'ten)
        self.rsi_exit_long: float = float(settings.get("short_thresh", 70))   # LONG kapat RSI>=
        self.rsi_exit_short: float = float(settings.get("long_thresh", 35))   # SHORT kapat RSI<=

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
                    "symbol": self.symbol, "interval": self.interval, "limit": 1000,
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

            # ── HA Reversal sinyal tespiti (kapanan mum icin) ──
            # bullSignal = haOpen == haLow (alt golge yok)
            # bearSignal = haOpen == haHigh (ust golge yok)
            self.prev_bull_signal = abs(ha_o - ha_l) < 1e-10
            self.prev_bear_signal = abs(ha_o - ha_h) < 1e-10

            await log.ainfo("ha_candle_closed", symbol=self.symbol,
                            ha_close=round(ha_c, 4), real_close=round(real_c, 4),
                            rsi=round(closed_rsi, 2) if closed_rsi else None,
                            bull=self.prev_bull_signal, bear=self.prev_bear_signal)

            # ── RSI Exit — ping-pong: HER IKI hesabi kontrol et ──
            if closed_rsi is not None:
                st = _get_acc_state(self.symbol)
                for acc in ["a", "b"]:
                    acc_side = st[acc]["side"]
                    if acc_side is None:
                        continue
                    if acc_side == "LONG" and closed_rsi >= self.rsi_exit_long:
                        await log.ainfo("ha_rsi_exit", symbol=self.symbol, account=acc.upper(),
                                        side="LONG", rsi=round(closed_rsi, 2))
                        await _close_account_position(self.symbol, acc, "RSI_EXIT")
                    elif acc_side == "SHORT" and closed_rsi <= self.rsi_exit_short:
                        await log.ainfo("ha_rsi_exit", symbol=self.symbol, account=acc.upper(),
                                        side="SHORT", rsi=round(closed_rsi, 2))
                        await _close_account_position(self.symbol, acc, "RSI_EXIT")

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

        # 5. HA Reversal giris kontrolu:
        # Onceki mum bullSignal ise → LONG sinyali (state != 1 ise)
        # Onceki mum bearSignal ise → SHORT sinyali (state != -1 ise)
        if not self.prev_bull_signal and not self.prev_bear_signal:
            return None

        # Pozisyon varsa ters sinyal kontrolu (AYNI yonde → skip, TERS yonde → sinyal ver)
        if self.prev_bull_signal:
            if self.has_position and self.position_side == "LONG":
                return None  # Zaten LONG — 2. hesap icin daha sonra
            # LONG sinyal
            entry_price = price  # market order, anlik fiyat
            self.signal_fired_this_bar = True
            self.last_signal_time = time.time()
            signal = {
                "symbol": self.symbol, "direction": "BUY", "entry_price": entry_price,
                "rsi_a": None, "rsi_b": None, "gap": None,
                "candle_a_time": self.candle_start, "source": "ha_server",
            }
            self.last_signal = signal
            return signal

        if self.prev_bear_signal:
            if self.has_position and self.position_side == "SHORT":
                return None  # Zaten SHORT — 2. hesap icin daha sonra
            # SHORT sinyal
            entry_price = price
            self.signal_fired_this_bar = True
            self.last_signal_time = time.time()
            signal = {
                "symbol": self.symbol, "direction": "SELL", "entry_price": entry_price,
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


## Ping-Pong hesap state — sembol bazli A/B pozisyon takibi
# {symbol: {"a": {"side":str|None, "entry":float, "qty":float, "time":int},
#            "b": {"side":str|None, "entry":float, "qty":float, "time":int}}}
_account_state: dict[str, dict] = {}


def _get_acc_state(sym: str) -> dict:
    if sym not in _account_state:
        _account_state[sym] = {
            "a": {"side": None, "entry": 0.0, "qty": 0.0, "time": 0},
            "b": {"side": None, "entry": 0.0, "qty": 0.0, "time": 0},
        }
    return _account_state[sym]


def _find_free_account(sym: str) -> str | None:
    """Bos hesap bul (A oncelikli)."""
    st = _get_acc_state(sym)
    if st["a"]["side"] is None:
        return "a"
    if st["b"]["side"] is None:
        return "b"
    return None


def _find_oldest_account(sym: str) -> str:
    """Ikisi de doluysa en eski pozisyonlu hesabi dondur."""
    st = _get_acc_state(sym)
    return "a" if st["a"]["time"] <= st["b"]["time"] else "b"


async def _close_account_position(sym: str, account: str, reason: str) -> None:
    """Belirtilen hesaptaki pozisyonu market close ile kapat."""
    from app.modules.binance_client import (
        place_market_order, cancel_all_open_orders, get_position_risk,
        place_market_order_b, cancel_all_open_orders_b, get_position_risk_b,
    )
    st = _get_acc_state(sym)
    acc = st[account]
    if acc["side"] is None:
        return

    try:
        if account == "a":
            positions = await get_position_risk(sym)
        else:
            positions = await get_position_risk_b(sym)

        pos_amt = 0.0
        for p in positions:
            if p.get("symbol") == sym:
                pos_amt = float(p.get("positionAmt", 0))
                break

        if pos_amt == 0:
            # Binance'ta zaten kapanmis
            acc["side"] = None
            acc["entry"] = 0.0
            acc["qty"] = 0.0
            return

        # Acik emirleri iptal (guvenlik SL varsa)
        try:
            if account == "a":
                await cancel_all_open_orders(sym)
            else:
                await cancel_all_open_orders_b(sym)
        except Exception:
            pass

        close_side = "SELL" if pos_amt > 0 else "BUY"
        qty = abs(pos_amt)

        if account == "a":
            await place_market_order(sym, close_side, qty, reduce_only=True)
        else:
            await place_market_order_b(sym, close_side, qty, reduce_only=True)

        await log.ainfo("ha_pingpong_closed", symbol=sym, account=account.upper(),
                        side=acc["side"], entry=acc["entry"], reason=reason)

    except Exception as e:
        await log.aerror("ha_pingpong_close_error", symbol=sym, account=account, error=str(e))

    # State temizle
    acc["side"] = None
    acc["entry"] = 0.0
    acc["qty"] = 0.0
    acc["time"] = 0


async def _open_account_position(sym: str, account: str, direction: str,
                                  engine: "HeikinAshiEngine", row_id: int) -> None:
    """Belirtilen hesaptan market order ile pozisyon ac. Guvenlik SL opsiyonel."""
    from app.modules.binance_client import (
        get_total_wallet_balance, get_total_wallet_balance_b,
        place_market_order, place_market_order_b,
        place_stop_market_instant, is_account_b_configured,
        round_price, round_step_size,
    )
    from app.modules.trade_executor import get_exchange_info_cached

    try:
        # Bakiye
        if account == "a":
            balance = await get_total_wallet_balance()
        else:
            balance = await get_total_wallet_balance_b()

        price = get_live_price(sym)
        if not price or price <= 0:
            return

        # Quantity hesapla
        info = await get_exchange_info_cached(sym)
        step_size = info["lotSize"]["stepSize"]
        min_qty = info["lotSize"]["minQty"]
        tick_size = info["priceFilter"]["tickSize"]

        sym_weight = engine.settings.get("weight", 0.10)
        usable = balance * sym_weight * 0.98
        raw_qty = usable / price
        quantity = round_step_size(raw_qty, step_size)

        if quantity < min_qty:
            await log.awarning("ha_pingpong_qty_low", symbol=sym, account=account, qty=quantity, min=min_qty)
            return

        # Market order
        side = "BUY" if direction == "BUY" else "SELL"
        if account == "a":
            result = await place_market_order(sym, side, quantity)
        else:
            result = await place_market_order_b(sym, side, quantity)

        fill_price = float(result.get("avgPrice", 0)) or price

        # State guncelle
        st = _get_acc_state(sym)
        st[account]["side"] = "LONG" if direction == "BUY" else "SHORT"
        st[account]["entry"] = fill_price
        st[account]["qty"] = quantity
        st[account]["time"] = int(time.time())

        await log.ainfo("ha_pingpong_opened", symbol=sym, account=account.upper(),
                        direction=direction, entry=round(fill_price, 6), qty=quantity)

        # ── Guvenlik SL (opsiyonel) ──
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
                from app.modules.binance_client import place_stop_market_instant
                if account == "a":
                    await place_stop_market_instant(sym, sl_side, quantity, sl_price)
                else:
                    # Hesap B icin SL — algoOrder API
                    from app.modules.binance_client import get_client_b, _sign_b
                    client_b = await get_client_b()
                    sl_params = _sign_b({
                        "symbol": sym, "side": sl_side, "type": "STOP_MARKET",
                        "algoType": "CONDITIONAL", "quantity": quantity, "triggerPrice": sl_price,
                    })
                    resp = await client_b.post("/fapi/v1/algoOrder", params=sl_params)
                await log.ainfo("ha_safety_sl_placed", symbol=sym, account=account.upper(),
                                sl_price=sl_price, sl_side=sl_side)
            except Exception as e:
                await log.awarning("ha_safety_sl_failed", symbol=sym, account=account, error=str(e))

    except Exception as e:
        await log.aerror("ha_pingpong_open_error", symbol=sym, account=account, error=str(e))


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
    from app.modules.binance_client import is_account_b_configured

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

    has_account_b = is_account_b_configured()
    await log.ainfo("ha_engine_accounts", account_a=True, account_b=has_account_b)

    last_pos_sync = time.time()
    last_settings_reload = time.time()

    try:
        while True:
            await asyncio.sleep(1)  # 1sn tick (rate limit dostu)
            now = time.time()

            # Pozisyon sync (60sn) — Binance'tan gercek durumu al
            if now - last_pos_sync > 60:
                last_pos_sync = now
                for sym, engine in _ha_engines.items():
                    try:
                        from app.modules.binance_client import get_position_risk, get_position_risk_b
                        # Hesap A sync
                        pos_a = await get_position_risk(sym)
                        for p in pos_a:
                            if p.get("symbol") == sym:
                                amt = float(p.get("positionAmt", 0))
                                st = _get_acc_state(sym)
                                if amt > 0:
                                    st["a"]["side"] = "LONG"
                                    if st["a"]["entry"] <= 0:
                                        st["a"]["entry"] = float(p.get("entryPrice", 0))
                                        st["a"]["qty"] = abs(amt)
                                elif amt < 0:
                                    st["a"]["side"] = "SHORT"
                                    if st["a"]["entry"] <= 0:
                                        st["a"]["entry"] = float(p.get("entryPrice", 0))
                                        st["a"]["qty"] = abs(amt)
                                else:
                                    st["a"]["side"] = None
                                break
                        # Hesap B sync
                        if has_account_b:
                            pos_b = await get_position_risk_b(sym)
                            for p in pos_b:
                                if p.get("symbol") == sym:
                                    amt = float(p.get("positionAmt", 0))
                                    st = _get_acc_state(sym)
                                    if amt > 0:
                                        st["b"]["side"] = "LONG"
                                        if st["b"]["entry"] <= 0:
                                            st["b"]["entry"] = float(p.get("entryPrice", 0))
                                            st["b"]["qty"] = abs(amt)
                                    elif amt < 0:
                                        st["b"]["side"] = "SHORT"
                                        if st["b"]["entry"] <= 0:
                                            st["b"]["entry"] = float(p.get("entryPrice", 0))
                                            st["b"]["qty"] = abs(amt)
                                    else:
                                        st["b"]["side"] = None
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
                            eng.rsi_exit_long = float(s.get("short_thresh", 70))
                            eng.rsi_exit_short = float(s.get("long_thresh", 35))
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
                        st = _get_acc_state(sym)
                        direction = signal["direction"]
                        new_side = "LONG" if direction == "BUY" else "SHORT"

                        # 1. Ters pozisyonlu hesaplari kapat
                        opposite = "SHORT" if new_side == "LONG" else "LONG"
                        for acc in ["a", "b"]:
                            if st[acc]["side"] == opposite:
                                await _close_account_position(sym, acc, "REVERSE")

                        # 2. Bos hesap bul → yeni pozisyon ac
                        free = _find_free_account(sym)
                        if free is None and has_account_b:
                            # Ikisi de dolu (ayni yonde) → en eski kapat
                            oldest = _find_oldest_account(sym)
                            await _close_account_position(sym, oldest, "ROTATION")
                            free = oldest
                        elif free is None:
                            # Tek hesap ve dolu → kapat + yeniden ac
                            await _close_account_position(sym, "a", "SINGLE_REOPEN")
                            free = "a"

                        if free == "b" and not has_account_b:
                            free = None  # 2. hesap yoksa skip

                        if free:
                            await _open_account_position(sym, free, direction, engine, row_id)

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
