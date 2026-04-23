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
_last_crash_error: str = ""


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
        # HA RSI sabit 10 — settings'teki rsi_len eski diverjans icin
        self.rsi_len = 10
        # HA state
        self.ha_prev_open: float = 0.0
        self.ha_prev_close: float = 0.0
        self.ha_candle_open: float = 0.0
        self.ha_candle_high: float = 0.0
        self.ha_candle_low: float = 0.0
        self.ha_candle_close: float = 0.0
        # Giris ve cikis hemen mum kapanisinda

    # ── Override: Warmup ────────────────────────────────
    async def warmup(self) -> None:
        """Binance'tan 200 normal mum cek → HA'ya donustur → RSI hesapla."""
        import httpx
        try:
            async with httpx.AsyncClient(timeout=30) as client:
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

            # RSI — simülasyonla ayni fonksiyon (calculate_rsi_with_state)
            # Son mum haric (canli mum) hesapla → RMA state'i motor'a aktar
            from app.modules.rsi_calculator import calculate_rsi_with_state
            closed_ha_closes = ha_closes[:-1]  # son mum haric
            rsi_values, rsi_state = calculate_rsi_with_state(closed_ha_closes, self.rsi_len)

            # RMA state'i motor'a aktar (simülasyonla birebir ayni)
            self.rsi_avg_gain = rsi_state["avg_gain"]
            self.rsi_avg_loss = rsi_state["avg_loss"]
            self.rsi_prev_close = rsi_state["prev_close"]
            self.rsi_warmed_up = True

            # Kapanmis HA mumlari (son mum haric)
            self.closed_candles = []
            for i in range(len(ha_candles) - 1):
                hc = ha_candles[i]
                hc["rsi"] = rsi_values[i] if i < len(rsi_values) else None
                hc["prev_rsi"] = rsi_values[i - 1] if i > 0 and i - 1 < len(rsi_values) else None
                self.closed_candles.append(hc)

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

    # ── Override: on_price_tick — HA Reversal + RSI Yon Filtresi v4 ──
    async def on_price_tick(self, price: float) -> dict | None:
        """v4: Tum kararlar kapanmis mumlarin RSI degerleriyle yapilir.
        Tick bazli RSI kontrolu YOK.

        Mum kapanisinda:
          1. HA hesapla → bull/bear signal tespit
          2. RSI hesapla → rsi_current vs rsi_prev (yon)
          3. CIKIS: LONG + RSI dustu → kapat emri (pending)
                    SHORT + RSI yukseldi → kapat emri (pending)
          4. GIRIS: bullSignal + RSI yukseliyor → ac emri (pending)
                    bearSignal + RSI dususyor → ac emri (pending)

        Sonraki mum ilk tick:
          1. Bekleyen kapat emri → MARKET order
          2. Bekleyen ac emri → sinyal dondur (loop trade acar)
        """
        if not self.warmed_up:
            return None

        now = int(time.time())

        # 1. Canli mum guncelle
        self.candle_high = max(self.candle_high, price)
        self.candle_low = min(self.candle_low, price)
        self.candle_close = price
        self._update_ha_candle()

        # 2. Mum kapandi mi?
        new_candle_start = (now // self.iv_sec) * self.iv_sec
        if new_candle_start > self.candle_start and now - new_candle_start > 5:
            # ── TOPLU HESAPLAMA: Binance'tan 1500 mum cek → HA → RSI ──
            # Kümülatif state TUTMA — her mum kapanisinda sifirdan hesapla
            # Simülasyonla birebir ayni sonuc, drift imkansiz
            try:
                import httpx
                from app.modules.rsi_calculator import calculate_rsi_with_state
                async with httpx.AsyncClient(timeout=30) as _client:
                    _resp = await _client.get(BINANCE_URL, params={
                        "symbol": self.symbol, "interval": self.interval, "limit": 1500,
                    })
                    _resp.raise_for_status()
                    klines = _resp.json()

                if klines and len(klines) >= self.rsi_len + 2:
                    # Son kapanan mumun zamani = candle_start olmali
                    last_closed_time = int(klines[-2][0]) // 1000
                    if last_closed_time != self.candle_start:
                        # Binance henuz kapanan mumu donmuyor — bekle
                        await log.awarning("ha_bar_close_time_mismatch",
                                          symbol=self.symbol,
                                          expected=self.candle_start,
                                          got=last_closed_time)
                        return None  # sonraki tick'te tekrar dene

                    ha_candles = convert_klines_to_ha(klines)
                    # Son mum haric (canli mum)
                    closed_ha = ha_candles[:-1]
                    ha_closes = [c["close"] for c in closed_ha]
                    rsi_values, rsi_state = calculate_rsi_with_state(ha_closes, self.rsi_len)

                    # RMA state guncelle (canli RSI icin)
                    self.rsi_avg_gain = rsi_state["avg_gain"]
                    self.rsi_avg_loss = rsi_state["avg_loss"]
                    self.rsi_prev_close = rsi_state["prev_close"]
                    self.rsi_warmed_up = True

                    # HA state guncelle
                    last = closed_ha[-1]
                    self.ha_prev_open = last["open"]
                    self.ha_prev_close = last["close"]

                    # Son kapanan mumun degerleri
                    ha_o = last["open"]
                    ha_h = last["high"]
                    ha_l = last["low"]
                    ha_c = last["close"]
                    real_c = last.get("real_close", ha_c)
                    real_o = last.get("real_open", ha_o)
                    closed_rsi = rsi_values[-1] if rsi_values and rsi_values[-1] is not None else None
                    prev_rsi = rsi_values[-2] if len(rsi_values) >= 2 and rsi_values[-2] is not None else None

                    # closed_candles guncelle
                    self.closed_candles = []
                    for i, hc in enumerate(closed_ha):
                        hc["rsi"] = rsi_values[i] if i < len(rsi_values) else None
                        hc["prev_rsi"] = rsi_values[i - 1] if i > 0 and i - 1 < len(rsi_values) else None
                        self.closed_candles.append(hc)
                    max_keep = max(self.max_gap + 20, 100)
                    if len(self.closed_candles) > max_keep:
                        self.closed_candles = self.closed_candles[-max_keep:]

                    # ── Bu mumun HA Reversal sinyali (sonraki mum icin) ──
                    tol = ha_o * 0.0005 if ha_o > 0 else 0.0005
                    new_bull = abs(ha_o - ha_l) <= tol
                    new_bear = abs(ha_o - ha_h) <= tol

                    # ── RSI yon tespiti (bu mumun RSI vs onceki) ──
                    rsi_up = False
                    rsi_down = False
                    if closed_rsi is not None and prev_rsi is not None:
                        rsi_up = closed_rsi > prev_rsi
                        rsi_down = closed_rsi < prev_rsi

                    await log.ainfo("ha_candle_closed", symbol=self.symbol,
                                    ha_o=round(ha_o, 4), ha_h=round(ha_h, 4),
                                    ha_l=round(ha_l, 4), ha_c=round(ha_c, 4),
                                    real_o=round(real_o, 4), real_c=round(real_c, 4),
                                    rsi=round(closed_rsi, 2) if closed_rsi else None,
                                    prev_rsi=round(prev_rsi, 2) if prev_rsi else None,
                                    rsi_up=rsi_up, rsi_down=rsi_down,
                                    tol=round(tol, 6),
                                    prev_bull=self.prev_bull_signal, prev_bear=self.prev_bear_signal,
                                    new_bull=new_bull, new_bear=new_bear)

                    # ── CIKIS — HEMEN (mum kapanisinda, pending degil) ──
                    st = _get_acc_state(self.symbol)
                    try:
                        for acc in ["a", "b"]:
                            acc_side = st[acc]["side"]
                            if acc_side is None:
                                continue
                            if acc_side == "LONG" and rsi_down:
                                await log.ainfo("ha_rsi_exit_now", symbol=self.symbol,
                                                side="LONG", reason="RSI_DOWN",
                                                rsi=round(closed_rsi, 2) if closed_rsi else None,
                                                prev_rsi=round(prev_rsi, 2) if prev_rsi else None)
                                await _close_account_position(self.symbol, acc, "RSI_DOWN")
                            elif acc_side == "SHORT" and rsi_up:
                                await log.ainfo("ha_rsi_exit_now", symbol=self.symbol,
                                                side="SHORT", reason="RSI_UP",
                                                rsi=round(closed_rsi, 2) if closed_rsi else None,
                                                prev_rsi=round(prev_rsi, 2) if prev_rsi else None)
                                await _close_account_position(self.symbol, acc, "RSI_UP")
                    except Exception as e:
                        await log.aerror("ha_exit_now_error", symbol=self.symbol, error=str(e))

                    # ── GIRIS — HEMEN (mum kapanisinda, ayni mumun sinyali) ──
                    st = _get_acc_state(self.symbol)  # cikis sonrasi guncellenmis state
                    effective_side = st["a"]["side"]
                    entry_signal = None
                    if new_bull and rsi_up and effective_side != "LONG":
                        entry_signal = "BUY"
                    elif new_bear and rsi_down and effective_side != "SHORT":
                        entry_signal = "SELL"

                    if entry_signal:
                        self.signal_fired_this_bar = True
                        self.last_signal_time = time.time()
                        signal = {
                            "symbol": self.symbol, "direction": entry_signal,
                            "entry_price": price,
                            "rsi_a": None, "rsi_b": None, "gap": None,
                            "candle_a_time": self.candle_start, "source": "ha_server",
                        }
                        self.last_signal = signal
                        await log.ainfo("ha_entry_now", symbol=self.symbol,
                                        direction=entry_signal,
                                        rsi=round(closed_rsi, 2) if closed_rsi else None)
                        # Yeni mum baslat (return'den ONCE)
                        self.candle_start = new_candle_start
                        self.candle_open = price
                        self.candle_high = price
                        self.candle_low = price
                        self.candle_close = price
                        self._update_ha_candle()
                        return signal

            except Exception as e:
                await log.aerror("ha_bar_close_error", symbol=self.symbol, error=str(e))

            # Yeni mum baslat (giris sinyali yoksa)
            self.candle_start = new_candle_start
            self.candle_open = price
            self.candle_high = price
            self.candle_low = price
            self.candle_close = price
            self._update_ha_candle()
            self.signal_fired_this_bar = False

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

        exit_price = get_live_price(sym) or 0
        entry_price = acc["entry"]
        pnl_pct = 0.0
        if entry_price > 0 and exit_price > 0:
            if acc["side"] == "LONG":
                pnl_pct = (exit_price - entry_price) / entry_price * 100
            elif acc["side"] == "SHORT":
                pnl_pct = (entry_price - exit_price) / entry_price * 100

        await log.ainfo("ha_pingpong_closed", symbol=sym, account=account.upper(),
                        side=acc["side"], entry=acc["entry"], exit=round(exit_price, 6),
                        pnl_pct=round(pnl_pct, 3), reason=reason)

        # Kapanışı st_signal_logger'a logla (sebep görünsün)
        try:
            from app.modules.st_signal_logger import log_st_signal
            from datetime import datetime, timezone, timedelta
            dt_str = datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")
            close_dir = "LONG" if acc["side"] == "LONG" else "SHORT"
            await log_st_signal(
                dt=dt_str, symbol=sym, direction=f"CLOSE_{close_dir}",
                band="15M", price=exit_price,
                entered=True, source=f"ha_close_{reason.lower()}",
            )
        except Exception:
            pass

        # State temizle — SADECE market order basarili olduysa
        acc["side"] = None
        acc["entry"] = 0.0
        acc["qty"] = 0.0
        acc["time"] = 0

    except Exception as e:
        # Market order BASARISIZ — state'e DOKUNMA (pozisyon hala acik olabilir)
        await log.aerror("ha_pingpong_close_error", symbol=sym, account=account, error=str(e))


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
        # Önce eski emirleri temizle (kalan SL varsa silinsin)
        try:
            from app.modules.binance_client import cancel_all_open_orders, cancel_all_open_orders_b
            if account == "a":
                await cancel_all_open_orders(sym)
            else:
                await cancel_all_open_orders_b(sym)
        except Exception:
            pass

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
        # Giriş anındaki RSI kaydet (OB/OS exit kontrolü için)
        _eng = _ha_engines.get(sym)
        st[account]["entry_rsi"] = _eng._calc_live_rsi(_eng.ha_candle_close) if _eng else 50

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
                from app.modules.binance_client import place_stop_market_instant, place_stop_market_instant_b
                if account == "a":
                    await place_stop_market_instant(sym, sl_side, quantity, sl_price)
                else:
                    await place_stop_market_instant_b(sym, sl_side, quantity, sl_price)
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
    global _last_crash_error
    try:
        all_settings = await get_all_settings()
        ha_syms = [s for s in all_settings if s.get("active") and s.get("ha_enabled")]
        await log.ainfo("ha_engine_loading", count=len(ha_syms),
                        symbols=[s["symbol"] for s in ha_syms])
        for s in ha_syms:
            sym = s["symbol"]
            try:
                engine = HeikinAshiEngine(sym, s)
                await engine.warmup()
                if engine.warmed_up:
                    _ha_engines[sym] = engine
                    await log.ainfo("ha_engine_started", symbol=sym)
                else:
                    _last_crash_error = f"warmup failed: {sym}"
                    await log.awarning("ha_warmup_failed", symbol=sym)
            except Exception as we:
                _last_crash_error = f"warmup {sym}: {we}"
                await log.aerror("ha_warmup_exception", symbol=sym, error=str(we))
            await asyncio.sleep(3)
    except Exception as e:
        _last_crash_error = f"init: {e}"
        await log.aerror("ha_engine_init_error", error=str(e))

    if not _ha_engines:
        await log.ainfo("ha_engine_no_symbols_yet")

    has_account_b = is_account_b_configured()
    await log.ainfo("ha_engine_accounts", account_a=True, account_b=has_account_b)

    # ── STARTUP SYNC: Binance'tan A/B pozisyonlari _account_state'e yukle ──
    from app.modules.binance_client import get_position_risk, get_position_risk_b
    for sym in list(_ha_engines.keys()):
        try:
            st = _get_acc_state(sym)
            pos_a = await get_position_risk(sym)
            for p in pos_a:
                if p.get("symbol") == sym:
                    amt = float(p.get("positionAmt", 0))
                    if amt > 0:
                        st["a"]["side"] = "LONG"
                        st["a"]["entry"] = float(p.get("entryPrice", 0))
                        st["a"]["qty"] = abs(amt)
                    elif amt < 0:
                        st["a"]["side"] = "SHORT"
                        st["a"]["entry"] = float(p.get("entryPrice", 0))
                        st["a"]["qty"] = abs(amt)
                    else:
                        st["a"]["side"] = None
                    break
            if has_account_b:
                pos_b = await get_position_risk_b(sym)
                for p in pos_b:
                    if p.get("symbol") == sym:
                        amt = float(p.get("positionAmt", 0))
                        if amt > 0:
                            st["b"]["side"] = "LONG"
                            st["b"]["entry"] = float(p.get("entryPrice", 0))
                            st["b"]["qty"] = abs(amt)
                        elif amt < 0:
                            st["b"]["side"] = "SHORT"
                            st["b"]["entry"] = float(p.get("entryPrice", 0))
                            st["b"]["qty"] = abs(amt)
                        else:
                            st["b"]["side"] = None
                        break
            await log.ainfo("ha_startup_sync", symbol=sym,
                            a_side=st["a"]["side"], b_side=st["b"]["side"])
        except Exception as e:
            await log.awarning("ha_startup_sync_error", symbol=sym, error=str(e))
        await asyncio.sleep(1)  # Rate limit

    last_pos_sync = time.time()
    last_settings_reload = time.time()

    try:
        while True:
            await asyncio.sleep(1)  # 1sn tick (rate limit dostu)
            now = time.time()

            # Pozisyon sync (300sn = 5dk) — Binance'tan gercek durumu al
            # 8 sembol × 2 hesap = 16 API call per sync — rate limit dostu
            if now - last_pos_sync > 300:
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
                            # eng.rsi_len sabit 10 — settings'ten override etme
                            eng.long_thresh = s.get("long_thresh", 32.0)
                            eng.short_thresh = s.get("short_thresh", 70.0)
                            eng.max_gap = s.get("max_gap", 21)
                            eng.entry_buffer = s.get("entry_buffer", 0.1) / 100.0
                            # RSI exit: yon karsilastirmasi (sabit threshold yok)
                        elif s.get("active") and s.get("ha_enabled") and sym not in _ha_engines:
                            try:
                                new_eng = HeikinAshiEngine(sym, s)
                                await new_eng.warmup()
                                if new_eng.warmed_up:
                                    _ha_engines[sym] = new_eng
                                    await log.ainfo("ha_engine_new_symbol", symbol=sym)
                                else:
                                    await log.awarning("ha_reload_warmup_failed", symbol=sym)
                            except Exception as we:
                                await log.aerror("ha_reload_warmup_error", symbol=sym, error=str(we))
                            await asyncio.sleep(3)  # Rate limit
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
                        # Pre-trade sync — Binance'tan gercek pozisyon
                        try:
                            from app.modules.binance_client import get_position_risk
                            st = _get_acc_state(sym)
                            pos_a = await get_position_risk(sym)
                            for p in pos_a:
                                if p.get("symbol") == sym:
                                    a_amt = float(p.get("positionAmt", 0))
                                    if a_amt > 0: st["a"]["side"] = "LONG"; st["a"]["qty"] = abs(a_amt)
                                    elif a_amt < 0: st["a"]["side"] = "SHORT"; st["a"]["qty"] = abs(a_amt)
                                    else: st["a"]["side"] = None
                                    break
                        except Exception as _sync_err:
                            await log.awarning("ha_pre_trade_sync_error", symbol=sym, error=str(_sync_err))

                        st = _get_acc_state(sym)
                        direction = signal["direction"]
                        new_side = "LONG" if direction == "BUY" else "SHORT"

                        # Ayni yonde acik → skip
                        if st["a"]["side"] == new_side:
                            await log.ainfo("ha_skip_same_side", symbol=sym, side=new_side)
                        else:
                            # Ters pozisyon varsa kapat
                            opposite = "SHORT" if new_side == "LONG" else "LONG"
                            if st["a"]["side"] == opposite:
                                await _close_account_position(sym, "a", "REVERSE")

                            # Pozisyon ac
                            st = _get_acc_state(sym)
                            if st["a"]["side"] is None:
                                await _open_account_position(sym, "a", direction, engine, row_id)

    except asyncio.CancelledError:
        await log.ainfo("ha_engine_loop_stopped")


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
