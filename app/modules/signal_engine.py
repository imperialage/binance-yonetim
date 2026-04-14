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
        self.used_a: set[int] = set()  # sadece isleme girildiginde eklenir
        self.warmed_up: bool = False
        self.last_signal_time: float = 0.0
        self.last_signal: dict | None = None  # son uretilen sinyal
        self.last_signal_bar: int = 0  # hangi mumda uretildi

        # TP/SL teyit state
        self.tp_confirmed: bool = False
        self.sl_confirmed: bool = False
        self.tp_price: float = 0.0
        self.sl_price: float = 0.0

        # Entry bilgileri — fill tespitinde doldurulur, kapanista kullanilir
        self.entry_price: float = 0.0
        self.entry_time: int = 0
        self.entry_qty: float = 0.0
        self.entry_event_id: str = ""
        self.entry_signal_id: int | None = None

        # Per-engine lock — pozisyon kapanisi + fill islemleri icin
        self._state_lock: asyncio.Lock = asyncio.Lock()

        # Pending order — fill takibi icin (trade_executor'dan gelir)
        self.pending_order: dict | None = None

        # Bar-close validation — webhook pozisyonunu mum kapanisinda dogrula
        self.webhook_entry_bar_time: int = 0
        self.webhook_entry_direction: str = ""  # "BUY" or "SELL"

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
                    "limit": 1000,
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

            # Leverage 1x — bir kez set et (trade_executor'dan kaldirildi)
            try:
                from app.modules.binance_client import set_leverage
                await set_leverage(self.symbol, leverage=1)
            except Exception:
                pass  # -4028 veya zaten set

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
        """Binance'tan gercek pozisyon durumunu al."""
        try:
            old_pos = self.has_position
            old_side = self.position_side
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
                    # Entry price ve qty yoksa Binance'tan al (restart/eski pozisyon)
                    if self.has_position and self.entry_price <= 0:
                        self.entry_price = float(p.get("entryPrice", 0))
                        self.entry_qty = abs(amt)
                    # TP/SL fiyatlari kayipsa (deploy restart) → Binance open algo orders'tan recover
                    if self.has_position and (self.tp_price <= 0 or self.sl_price <= 0):
                        try:
                            from app.modules.binance_client import get_algo_orders_history
                            algo_orders = await get_algo_orders_history(self.symbol)
                            for ao in algo_orders:
                                if ao.get("algoStatus") not in ("WORKING", "NEW"):
                                    continue
                                trigger = float(ao.get("triggerPrice", 0))
                                if trigger <= 0:
                                    continue
                                ao_type = ao.get("type", "")
                                if ao_type == "TAKE_PROFIT_MARKET" and self.tp_price <= 0:
                                    self.tp_price = trigger
                                    self.tp_confirmed = True
                                    await log.ainfo("tp_recovered_from_binance", symbol=self.symbol, tp=trigger)
                                elif ao_type == "STOP_MARKET" and self.sl_price <= 0:
                                    self.sl_price = trigger
                                    self.sl_confirmed = True
                                    await log.ainfo("sl_recovered_from_binance", symbol=self.symbol, sl=trigger)
                        except Exception as e:
                            await log.awarning("tpsl_recovery_failed", symbol=self.symbol, error=str(e))
                    # State degisti mi logla
                    if old_pos != self.has_position or old_side != self.position_side:
                        await log.ainfo(
                            "position_state_changed",
                            symbol=self.symbol,
                            old_pos=old_pos,
                            old_side=old_side,
                            new_pos=self.has_position,
                            new_side=self.position_side,
                        )
                    return
            self.has_position = False
            self.position_side = ""
            if not self.pending_order:
                self.trade_pending = False
            if old_pos != self.has_position:
                await log.ainfo("position_state_changed", symbol=self.symbol, old_pos=old_pos, new_pos=False)
        except Exception as e:
            await log.awarning("signal_engine_position_check_error", symbol=self.symbol, error=str(e))

    # ── Pozisyon kapandi bildirimi (order_stream'den) ────
    def on_position_closed(self, exit_info: dict | None = None) -> None:
        """TP/SL fill → pozisyon kapandi. exit_info: order_stream'den gelen detaylar."""
        # Kapanış kaydı için mevcut state'i sakla
        prev_direction = self.position_side  # LONG/SHORT
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
        self.webhook_entry_bar_time = 0
        self.webhook_entry_direction = ""
        # algo_ids.json temizle — eski TP/SL ID'leri kalmasin
        try:
            from app.modules.binance_client import _load_algo_ids, _save_algo_ids
            data = _load_algo_ids()
            if self.symbol in data:
                data[self.symbol] = []
                _save_algo_ids(data)
        except Exception:
            pass

        # trade_closures kaydı oluştur (async task olarak)
        if exit_info and prev_direction:
            entry_data = {
                "entry_price": prev_entry_price,
                "entry_time": prev_entry_time,
                "qty": prev_entry_qty,
                "event_id": prev_entry_event_id,
                "signal_id": prev_entry_signal_id,
            }
            asyncio.create_task(
                self._record_closure(exit_info, prev_direction, prev_tp, prev_sl, entry_data)
            )

    async def _record_closure(
        self,
        exit_info: dict,
        direction: str,
        tp_price: float,
        sl_price: float,
        entry_data: dict,
    ) -> None:
        """trade_closures tablosuna kapanış kaydı yaz."""
        try:
            from app.modules.trade_store import log_closure

            exit_price = exit_info.get("avg_price", 0)
            exit_time = int(time.time())
            qty = exit_info.get("qty", 0) or entry_data.get("qty", 0)
            realized_pnl = exit_info.get("realized_pnl", 0)

            # Entry bilgisini kalici alanlardan al
            entry_price = entry_data.get("entry_price", 0)
            entry_time = entry_data.get("entry_time", 0)
            event_id = entry_data.get("event_id", "")
            signal_id = entry_data.get("signal_id")

            # Entry bilgisi yoksa exit fiyatini kullan (fallback)
            if entry_price <= 0:
                entry_price = exit_price

            # exit_reason belirle: çıkış fiyatı TP'ye mi SL'ye mi yakın?
            exit_reason = exit_info.get("exit_reason", "")
            if not exit_reason and tp_price > 0 and sl_price > 0 and exit_price > 0:
                tp_dist = abs(exit_price - tp_price)
                sl_dist = abs(exit_price - sl_price)
                if tp_dist <= sl_dist:
                    exit_reason = "TP"
                else:
                    exit_reason = "SL"
            if not exit_reason:
                order_type = exit_info.get("order_type", "")
                if "TAKE_PROFIT" in order_type:
                    exit_reason = "TP"
                elif "STOP" in order_type:
                    exit_reason = "SL"
                elif "MARKET" in order_type:
                    exit_reason = "MANUAL"
                else:
                    exit_reason = "UNKNOWN"

            # PnL hesapla
            if direction == "LONG":
                pnl_pct = (exit_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
            else:
                pnl_pct = (entry_price - exit_price) / entry_price * 100 if entry_price > 0 else 0
            pnl_usdt = realized_pnl if realized_pnl else pnl_pct / 100 * entry_price * qty

            hold_seconds = exit_time - entry_time if entry_time > 0 else 0

            closure_id = await log_closure(
                event_id=event_id,
                signal_id=signal_id,
                symbol=self.symbol,
                direction=direction,
                entry_price=entry_price,
                entry_time=entry_time,
                exit_price=exit_price,
                exit_time=exit_time,
                exit_reason=exit_reason,
                qty=qty,
                pnl_usdt=round(pnl_usdt, 4),
                pnl_pct=round(pnl_pct, 4),
                commission=0.04 * 2,  # %0.04 giriş + çıkış
                hold_duration_seconds=hold_seconds,
                tp_price=tp_price,
                sl_price=sl_price,
            )

            await log.ainfo(
                "trade_closure_recorded",
                symbol=self.symbol,
                direction=direction,
                exit_reason=exit_reason,
                pnl_pct=round(pnl_pct, 2),
                closure_id=closure_id,
            )

            # SL kapanışlarında post-exit analiz başlat
            if exit_reason == "SL" and closure_id > 0:
                asyncio.create_task(self._collect_post_exit_candles(closure_id, exit_time))

        except Exception as e:
            log.error("record_closure_failed", symbol=self.symbol, error=str(e))

    async def _collect_post_exit_candles(self, closure_id: int, exit_time: int) -> None:
        """SL kapanışından sonra 20 mum boyunca fiyat hareketini kaydet."""
        try:
            import httpx
            from app.modules.trade_store import log_post_exit_candles

            # 5 dakika bekle, sonra mumları çek (bir kaç mum oluşsun)
            await asyncio.sleep(300)

            iv = self.interval
            iv_ms = self.iv_sec * 1000
            start_ms = exit_time * 1000
            end_ms = start_ms + (20 * iv_ms)

            proxy_url = None
            try:
                from app.config import settings as app_cfg
                proxy_url = app_cfg.binance_proxy_url or None
            except Exception:
                pass

            async with httpx.AsyncClient(timeout=15, proxy=proxy_url) as client:
                resp = await client.get("https://fapi.binance.com/fapi/v1/klines", params={
                    "symbol": self.symbol, "interval": iv,
                    "startTime": start_ms, "endTime": end_ms, "limit": 20,
                })
                resp.raise_for_status()
                klines = resp.json()

            candles = []
            for i, k in enumerate(klines):
                candles.append({
                    "index": i + 1,
                    "open_time": int(k[0]) // 1000,
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                })

            if candles:
                await log_post_exit_candles(closure_id, candles)
                await log.ainfo("post_exit_candles_saved", symbol=self.symbol, closure_id=closure_id, count=len(candles))
        except Exception as e:
            await log.awarning("post_exit_candles_failed", symbol=self.symbol, error=str(e))

    def on_position_opened(self, side: str) -> None:
        """Limit order fill → pozisyon acildi."""
        self.has_position = True
        self.position_side = side
        # pending_order doluysa trade_pending'e dokunma
        # check_pending_fill TP/SL koyacak
        if not self.pending_order:
            self.trade_pending = False

    async def _bar_close_invalidate(self) -> None:
        """Mum kapanisinda sinyal gecersiz → pozisyonu market close."""
        try:
            from app.modules.binance_client import place_market_order, cancel_all_open_orders, get_position_risk
            positions = await get_position_risk(self.symbol)
            pos_amt = 0.0
            for p in positions:
                if p.get("symbol") == self.symbol:
                    pos_amt = float(p.get("positionAmt", 0))
                    break
            if pos_amt == 0:
                return
            await cancel_all_open_orders(self.symbol)
            close_side = "SELL" if pos_amt > 0 else "BUY"
            await place_market_order(self.symbol, close_side, abs(pos_amt), reduce_only=True)
            self.on_position_closed(exit_info={
                "avg_price": get_live_price(self.symbol) or 0,
                "qty": abs(pos_amt),
                "order_type": "MARKET",
                "realized_pnl": 0,
                "exit_reason": "BAR_CLOSE_INVALIDATED",
            })
            await log.ainfo("bar_close_signal_invalidated", symbol=self.symbol, pos_amt=pos_amt)
        except Exception as e:
            await log.aerror("bar_close_invalidate_failed", symbol=self.symbol, error=str(e))

    def on_trade_pending(self) -> None:
        """execute_trade cagrildi, fill bekleniyor."""
        self.trade_pending = True

    async def try_execute_signal(self, signal: dict) -> bool:
        """Sinyal geldi — pozisyon yoksa isleme gir, varsa skip. Returns True if executed."""
        if self.has_position:
            await log.ainfo("signal_skipped_has_position", symbol=self.symbol, direction=signal["direction"])
            return False
        if self.trade_pending:
            await log.ainfo("signal_skipped_trade_pending", symbol=self.symbol, direction=signal["direction"])
            return False

        # Isleme gir — used_a'ya SIMDI ekle
        a_time = signal.get("candle_a_time")
        if a_time:
            self.used_a.add(a_time)
            self._save_used_a()

        return True  # _engine_loop execute_trade cagirsin

    async def check_position_closed(self) -> None:
        """Pozisyon acikken kapanmis mi kontrol et. Kapandiysa eski emirleri temizle."""
        async with self._state_lock:
            if not self.has_position:
                return  # zaten kapali — order_stream halletmis olabilir
            try:
                positions = await get_position_risk(self.symbol)
                pos_amt = 0.0
                for p in positions:
                    if p.get("symbol") == self.symbol:
                        pos_amt = float(p.get("positionAmt", 0))
                        break

                if pos_amt != 0:
                    return  # pozisyon hala acik

                # Pozisyon kapanmis! Eski emirleri temizle
                from app.modules.binance_client import cancel_all_open_orders
                try:
                    await cancel_all_open_orders(self.symbol)
                    await log.ainfo("position_closed_orders_cleaned", symbol=self.symbol)
                except Exception:
                    pass

                # Polling ile tespit — exit bilgisini live price'dan al
                _exit_price = get_live_price(self.symbol) or 0
                self.on_position_closed(exit_info={
                    "avg_price": _exit_price,
                    "qty": 0,
                    "order_type": "",
                    "realized_pnl": 0,
                })
                await log.ainfo("position_closed_detected", symbol=self.symbol)
                # last_signal mekanizmasi kaldirildi — Pine Script uyumlu
                # Pozisyon kapandiginda yeni sinyal beklenir, eski sinyal tetiklenmez

            except Exception as e:
                await log.awarning("check_position_closed_error", symbol=self.symbol, error=str(e))

    async def check_pending_fill(self) -> None:
        """trade_pending ise: Binance'tan pozisyon kontrol et, fill olduysa TP/SL koy."""
        if not self.trade_pending or not self.pending_order:
            return

        async with self._state_lock:
            if not self.trade_pending or not self.pending_order:
                return  # lock beklerken order_stream halletmis olabilir
            await self._check_pending_fill_inner()

    async def _check_pending_fill_inner(self) -> None:
        """check_pending_fill ic mantigi — _state_lock altinda cagirilir."""
        po = self.pending_order

        # Timeout kontrolu (15dk)
        elapsed = time.time() - po["start_time"]
        if elapsed > po["timeout"]:
            # Tum emirleri iptal et (limit entry + algo SL dahil)
            try:
                from app.modules.binance_client import cancel_all_open_orders
                result = await cancel_all_open_orders(self.symbol)
                await log.ainfo("pending_timeout_all_cancelled", symbol=self.symbol, result=result)
            except Exception as e:
                await log.awarning("pending_timeout_cancel_failed", symbol=self.symbol, error=str(e))
            # Sinyal kaydini guncelle
            try:
                from app.modules.st_signal_logger import get_db as get_signal_db
                db = await get_signal_db()
                parts = po.get("event_id", "").split("-")
                if len(parts) >= 2:
                    row_id = int(parts[1])
                    await db.execute(
                        "UPDATE signal_log SET skip_reason = ?, entered = 0 WHERE id = ?",
                        ("Fiyat olusmadi - 15dk timeout", row_id),
                    )
                    await db.commit()
            except Exception:
                pass
            self.trade_pending = False
            self.pending_order = None
            await log.ainfo("pending_timeout", symbol=self.symbol, elapsed=int(elapsed))
            return

        # Limit order hala aktif mi kontrol et — iptal edildiyse SL'yi de temizle
        try:
            from app.modules.binance_client import get_order_status, cancel_all_open_orders as _cancel_all
            order_info = await get_order_status(self.symbol, int(po["order_id"]))
            order_status = order_info.get("status", "")
            if order_status in ("CANCELED", "EXPIRED", "REJECTED"):
                # Limit order iptal/expire olmus — SL dahil tum emirleri temizle
                await log.ainfo("pending_entry_cancelled", symbol=self.symbol, status=order_status, order_id=po["order_id"])
                try:
                    await _cancel_all(self.symbol)
                except Exception:
                    pass
                self.trade_pending = False
                self.pending_order = None
                return
        except Exception:
            pass  # order status alinamazsa pozisyon kontrolune devam et

        # Binance'tan gercek pozisyon kontrol
        try:
            from app.modules.binance_client import get_position_risk
            positions = await get_position_risk(self.symbol)
            pos_amt = 0.0
            entry_price = 0.0
            for p in positions:
                if p.get("symbol") == self.symbol:
                    pos_amt = float(p.get("positionAmt", 0))
                    entry_price = float(p.get("entryPrice", 0))
                    break

            if pos_amt == 0:
                return  # henuz dolmamis, beklemeye devam

            # FILL OLMUS! Gercek bilgiler:
            qty = abs(pos_amt)
            side = po["side"]
            is_long = pos_amt > 0
            tick_size = po["tick_size"]
            sl_enabled = po["sl_enabled"]

            await log.ainfo(
                "pending_fill_detected",
                symbol=self.symbol,
                side=side,
                entry_price=entry_price,
                qty=qty,
            )

            # Pozisyon state guncelle
            self.has_position = True
            self.position_side = "LONG" if is_long else "SHORT"
            self.trade_pending = False

            # Entry bilgilerini kalici alanlara kaydet (kapanista kullanilacak)
            self.entry_price = entry_price
            self.entry_time = int(time.time())
            self.entry_qty = qty
            self.entry_event_id = po.get("event_id", "")
            self.entry_signal_id = po.get("signal_id")

            self.pending_order = None

            # TP/SL hesapla — webhook'tan geldiyse onu kullan, yoksa indicator_settings'ten
            from app.modules.indicator_settings_store import get_settings_or_defaults
            from app.modules.binance_client import (
                round_price, place_take_profit_market_order,
                place_stop_market_order, place_market_order,
                cancel_all_open_orders,
            )
            sym_cfg = await get_settings_or_defaults(self.symbol)

            # TP/SL SADECE webhook'tan (Pine Script) — baska kaynaktan GELMEZ
            webhook_tp_val = po.get("webhook_tp_price")
            webhook_sl_val = po.get("webhook_sl_price")
            tp_price = round_price(webhook_tp_val, tick_size) if webhook_tp_val else None
            sl_price = round_price(webhook_sl_val, tick_size) if webhook_sl_val else None

            exit_side = "SELL" if is_long else "BUY"

            # Eski TP/SL emirlerini temizle (yeni SL zaten giris emriyle kondu)
            try:
                await cancel_all_open_orders(self.symbol)
                await log.ainfo("old_orders_cleaned", symbol=self.symbol)
            except Exception:
                pass

            # Yeni SL'yi tekrar koy (cancel_all ile silindi) — SADECE webhook'tan geldiyse
            if sl_enabled and sl_price:
                try:
                    from app.modules.binance_client import place_stop_market_instant
                    await place_stop_market_instant(self.symbol, exit_side, qty, sl_price)
                    self.sl_confirmed = True
                    self.sl_price = sl_price
                    await log.ainfo("sl_re_placed_after_cleanup", symbol=self.symbol, sl_price=sl_price)
                except Exception as e:
                    self.sl_confirmed = False
                    await log.aerror("sl_re_place_failed", symbol=self.symbol, error=str(e))
            elif not sl_price:
                await log.ainfo("sl_skipped_no_webhook_price", symbol=self.symbol)

            # TP koy — SADECE webhook'tan geldiyse
            if not tp_price:
                await log.ainfo("tp_skipped_no_webhook_price", symbol=self.symbol)
            else:
                try:
                    await place_take_profit_market_order(self.symbol, exit_side, qty, tp_price)
                    self.tp_confirmed = True
                    self.tp_price = tp_price
                    await log.ainfo("tp_placed", symbol=self.symbol, tp_price=tp_price)
                except Exception as e:
                    err = str(e)
                    await log.aerror("tp_place_failed", symbol=self.symbol, error=err)
                    if "-2021" in err:
                        # Fiyat TP'yi asmis — market ile kapat (karda)
                        try:
                            await cancel_all_open_orders(self.symbol)
                            await place_market_order(self.symbol, exit_side, qty, reduce_only=True)
                            _p = get_live_price(self.symbol) or tp_price
                            self.on_position_closed(exit_info={"avg_price": _p, "qty": qty, "order_type": "TAKE_PROFIT_MARKET", "realized_pnl": 0, "exit_reason": "TP"})
                            await log.ainfo("tp_passed_market_close", symbol=self.symbol)
                        except Exception as e2:
                            await log.aerror("tp_passed_close_failed", symbol=self.symbol, error=str(e2))
                        return

            # SL yedek — SADECE webhook SL varsa ve henuz konmadiysa
            if not self.sl_confirmed and sl_enabled and sl_price:
                try:
                    await place_stop_market_order(self.symbol, exit_side, qty, sl_price)
                    self.sl_confirmed = True
                    self.sl_price = sl_price
                    await log.ainfo("sl_placed_backup", symbol=self.symbol, sl_price=sl_price)
                except Exception as e:
                    err = str(e)
                    await log.aerror("sl_place_failed", symbol=self.symbol, error=err)
                    if "-2021" in err:
                        try:
                            await cancel_all_open_orders(self.symbol)
                            await place_market_order(self.symbol, exit_side, qty, reduce_only=True)
                            _p2 = get_live_price(self.symbol) or sl_price
                            self.on_position_closed(exit_info={"avg_price": _p2, "qty": qty, "order_type": "STOP_MARKET", "realized_pnl": 0, "exit_reason": "SL"})
                            await log.ainfo("sl_passed_market_close", symbol=self.symbol)
                        except Exception as e2:
                            await log.aerror("sl_passed_close_failed", symbol=self.symbol, error=str(e2))
                        return
                    # SL hicbir sekilde konamadi — pozisyonu market ile kapat
                    await log.aerror(
                        "sl_all_attempts_failed_closing_position",
                        symbol=self.symbol,
                        sl_price=sl_price,
                        error=err,
                    )
                    try:
                        await cancel_all_open_orders(self.symbol)
                        await place_market_order(self.symbol, exit_side, qty, reduce_only=True)
                        _p3 = get_live_price(self.symbol) or 0
                        self.on_position_closed(exit_info={"avg_price": _p3, "qty": qty, "order_type": "MARKET", "realized_pnl": 0, "exit_reason": "WATCHDOG"})
                        await log.ainfo("sl_failed_emergency_close", symbol=self.symbol)
                    except Exception as e3:
                        await log.aerror("sl_failed_emergency_close_failed", symbol=self.symbol, error=str(e3))
                    return
            elif not sl_enabled:
                self.sl_confirmed = True

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
                closed_previous=po.get("closed_previous", False),
                balance_used=po.get("balance", 0),
                signal_id=po.get("signal_id"),
            )

        except Exception as e:
            await log.aerror("check_pending_fill_error", symbol=self.symbol, error=str(e))

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
            # Binance'tan gercek son kapanmis mumu cek (RSI drift fix)
            real_close = self.candle_close
            real_high = self.candle_high
            real_low = self.candle_low
            real_open = self.candle_open
            try:
                import httpx
                proxy_url = None
                try:
                    from app.config import settings as _s
                    proxy_url = _s.binance_proxy_url or None
                except Exception:
                    pass
                async with httpx.AsyncClient(timeout=5, proxy=proxy_url) as _c:
                    _resp = await _c.get(BINANCE_URL, params={
                        "symbol": self.symbol, "interval": self.interval, "limit": 2,
                    })
                    if _resp.is_success:
                        _kl = _resp.json()
                        if _kl and len(_kl) >= 2:
                            # Son kapanmis mum = _kl[0] (ilk eleman)
                            real_open = float(_kl[0][1])
                            real_high = float(_kl[0][2])
                            real_low = float(_kl[0][3])
                            real_close = float(_kl[0][4])
            except Exception:
                pass  # basarisiz olursa tick fiyati kullan

            # Kapanan mumun RSI'ini GERCEK close ile hesapla
            closed_rsi = self._calc_live_rsi(real_close)
            self._advance_candle(real_close)

            # Kapanan mumu kaydet (gercek OHLC)
            closed = {
                "time": self.candle_start,
                "open": real_open,
                "high": real_high,
                "low": real_low,
                "close": real_close,
                "rsi": closed_rsi,
            }
            self.closed_candles.append(closed)
            # Diverjans icin max_gap yeterli, debug icin biraz fazla tut
            max_keep = max(self.max_gap + 20, 100)
            if len(self.closed_candles) > max_keep:
                self.closed_candles = self.closed_candles[-max_keep:]

            # Mum kapanisi logu — potansiyel A mumlarini say
            ob_count = sum(1 for c in self.closed_candles[-self.max_gap:] if c.get("rsi") and c["rsi"] >= self.short_thresh and c["time"] not in self.used_a)
            os_count = sum(1 for c in self.closed_candles[-self.max_gap:] if c.get("rsi") and c["rsi"] <= self.long_thresh and c["time"] not in self.used_a)
            await log.ainfo(
                "candle_closed",
                symbol=self.symbol,
                close=round(self.candle_close, 4),
                high=round(closed["high"], 4),
                low=round(closed["low"], 4),
                rsi=round(closed_rsi, 2) if closed_rsi else None,
                has_position=self.has_position,
                trade_pending=self.trade_pending,
                position_side=self.position_side,
                signal_fired=self.signal_fired_this_bar,
                ob_candidates=ob_count,
                os_candidates=os_count,
                used_a=len(self.used_a),
            )

            # Pine Script uyumlu: kapanan mum B olarak tekrar test edilmiyor
            # Sadece canli mum (sonraki bar) B olarak _check_divergence'da test edilir

            # ── Bar close validation — webhook pozisyonunu dogrula ──
            if self.has_position and self.webhook_entry_bar_time > 0:
                old_candle_start = self.candle_start
                if self.webhook_entry_bar_time == old_candle_start:
                    # Pozisyon bu mumda acildi — kapanan mumun verileriyle divergence check
                    # usedA ATLANIR: mevcut A mumu zaten usedA'da, tekrar bulabilmeli
                    sig = self._check_divergence_for_validation(real_close, closed_rsi) if closed_rsi is not None else None
                    signal_still_valid = (sig is not None and sig["direction"] == self.webhook_entry_direction)
                    if not signal_still_valid:
                        await log.ainfo("bar_close_validation_failed", symbol=self.symbol,
                                        direction=self.webhook_entry_direction,
                                        closed_rsi=round(closed_rsi, 2) if closed_rsi else None)
                        await self._bar_close_invalidate()
                        self.webhook_entry_bar_time = 0
                        self.webhook_entry_direction = ""
                    else:
                        await log.ainfo("bar_close_validation_passed", symbol=self.symbol,
                                        direction=self.webhook_entry_direction)
                        self.webhook_entry_bar_time = 0
                        self.webhook_entry_direction = ""

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

        # ── 4. Pozisyon kontrolu — Pine Script uyumlu: pozisyon varsa sinyal ARAMA ──
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
            self.last_signal = signal
            self.last_signal_bar = self.candle_start
            return signal

        return None

    def _check_divergence_for_validation(self, current_price: float, live_rsi: float) -> dict | None:
        """Bar close validation icin divergence check — usedA ATLANIR.

        Mevcut pozisyonun A mumu zaten usedA'da, bunu tekrar bulmak icin
        usedA kontrolunu atlamak gerekir. Sadece koşullari (RSI, high/low) kontrol eder.
        """
        n = len(self.closed_candles)
        search_range = min(self.max_gap, n)
        allowed = self.settings.get("allowed_directions", "BOTH")
        if allowed in ("BOTH", "SELL"):
            for gap in range(1, search_range + 1):
                a = self.closed_candles[n - gap]
                if a["rsi"] is None:
                    continue
                # usedA KONTROL EDILMEZ — validation icin
                if (a["rsi"] >= self.short_thresh
                        and self.candle_high > a["high"]
                        and live_rsi < a["rsi"]):
                    return {"direction": "SELL"}
        if allowed in ("BOTH", "BUY"):
            for gap in range(1, search_range + 1):
                a = self.closed_candles[n - gap]
                if a["rsi"] is None:
                    continue
                if (a["rsi"] <= self.long_thresh
                        and self.candle_low < a["low"]
                        and live_rsi > a["rsi"]):
                    return {"direction": "BUY"}
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
                    # used_a'ya EKLENMEZ — sadece isleme girildiginde eklenir
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
                    # used_a'ya EKLENMEZ — sadece isleme girildiginde eklenir
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
        # Yukleme sirasinda eski kayitlari temizle
        self._cleanup_used_a()

    def _cleanup_used_a(self) -> None:
        """24 saatten eski used_a kayitlarini temizle."""
        cutoff = int(time.time()) - 86400  # 24 saat
        before = len(self.used_a)
        self.used_a = {ts for ts in self.used_a if ts > cutoff}
        removed = before - len(self.used_a)
        if removed > 0:
            self._save_used_a()

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

async def _cleanup_orphan_limit_orders() -> None:
    """Periyodik: pozisyon yokken VE pending_order yokken kalan TUM emirleri temizle.

    Ozellikle: reduceOnly=false SL algo order'lari — giris emri iptal edilmis
    ama SL hala duruyorsa, ters pozisyon acabilir.
    """
    from app.modules.binance_client import get_position_risk, cancel_all_open_orders

    for sym, engine in _engines.items():
        # trade_pending veya pending_order varsa → bu emirler bizim, dokunma
        if engine.trade_pending or engine.pending_order:
            continue

        try:
            # Pozisyon var mi?
            positions = await get_position_risk(sym)
            pos_amt = 0.0
            for p in positions:
                if p.get("symbol") == sym:
                    pos_amt = float(p.get("positionAmt", 0))
                    break

            # Pozisyon yokken TUM emirleri temizle (LIMIT + algo SL/TP)
            if pos_amt == 0:
                try:
                    result = await cancel_all_open_orders(sym)
                    total = result.get("total", 0)
                    if total > 0:
                        await log.ainfo("orphan_all_orders_cancelled", symbol=sym, result=result)
                except Exception as e:
                    await log.awarning("orphan_cleanup_cancel_error", symbol=sym, error=str(e))

        except Exception as e:
            await log.awarning("orphan_cleanup_error", symbol=sym, error=str(e))


async def _watchdog_market_close(symbol: str, side: str, qty: float, reason: str) -> None:
    """Fiyat TP/SL'yi asmis — pozisyonu market ile kapat."""
    from app.modules.binance_client import place_market_order, cancel_all_open_orders
    try:
        # Once mevcut emirleri temizle
        try:
            await cancel_all_open_orders(symbol)
        except Exception:
            pass
        # Market ile kapat
        result = await place_market_order(symbol, side, qty, reduce_only=True)
        await log.ainfo(
            "watchdog_market_close",
            symbol=symbol,
            side=side,
            qty=qty,
            reason=reason,
            order_id=result.get("orderId", ""),
        )
        # Engine state guncelle
        engine = _engines.get(symbol)
        if engine:
            engine.on_position_closed()
    except Exception as e:
        await log.aerror("watchdog_market_close_failed", symbol=symbol, reason=reason, error=str(e))


async def _tpsl_watchdog() -> None:
    """Acik pozisyonlar icin TP/SL emirlerini kontrol et — eksikse koy.

    TEYITLI olana kadar her 20sn'de bir dener, ASLA vazgecmez.
    """
    from app.modules.binance_client import (
        get_position_risk, get_all_orders,
        place_take_profit_market_order, place_stop_market_order,
        get_exchange_info,
    )
    from app.modules.indicator_settings_store import get_settings_or_defaults

    for sym, engine in _engines.items():
        if not engine.has_position:
            continue

        # Ikisi de teyitli ise kontrol etmeye gerek yok
        sl_needed = bool(engine.settings.get("sl_enabled", 1))
        if engine.tp_confirmed and (engine.sl_confirmed or not sl_needed):
            continue

        try:
            # 1. Binance'tan pozisyon bilgisi
            positions = await get_position_risk(sym)
            pos_amt = 0.0
            entry_price = 0.0
            for p in positions:
                if p.get("symbol") == sym:
                    pos_amt = float(p.get("positionAmt", 0))
                    entry_price = float(p.get("entryPrice", 0))
                    break

            if pos_amt == 0 or entry_price <= 0:
                continue

            qty = abs(pos_amt)
            is_long = pos_amt > 0
            exit_side = "SELL" if is_long else "BUY"

            # 2. Mevcut açık emirleri kontrol et — algo_ids.json (kalici, deploy-safe)
            has_tp = False
            has_sl = False

            try:
                from app.modules.binance_client import _load_algo_ids
                algo_data = _load_algo_ids()
                sym_algo_count = len(algo_data.get(sym, []))
                # 2+ algo ID = TP + SL konmus
                if sym_algo_count >= 2:
                    has_tp = True
                    has_sl = True
                elif sym_algo_count == 1:
                    has_tp = True  # en az TP konmus
            except Exception:
                pass

            # Teyit flag'lerini guncelle
            engine.tp_confirmed = has_tp
            engine.sl_confirmed = has_sl or not sl_needed

            if engine.tp_confirmed and engine.sl_confirmed:
                continue  # hepsi teyitli, sorun yok

            # 3. TP/SL fiyatlarını hesapla — engine'de kayitli varsa onu kullan (webhook'tan gelmis olabilir)
            settings = await get_settings_or_defaults(sym)

            # tick_size al
            try:
                info = await get_exchange_info(sym)
                tick_size = float(info.get("priceFilter", {}).get("tickSize", 0.0001))
            except Exception:
                tick_size = 0.0001

            from app.modules.binance_client import round_price

            # TP/SL SADECE webhook'tan (Pine Script) — engine'de kayitliysa onu kullan
            # indicator_settings'ten hesaplama YAPILMAZ (tek kaynak: webhook)
            tp_price = engine.tp_price if engine.tp_price > 0 else None
            sl_price = engine.sl_price if engine.sl_price > 0 else None

            # 4. Eksik emirleri koy — SADECE webhook fiyati varsa
            if not has_tp and tp_price:
                try:
                    await place_take_profit_market_order(sym, exit_side, qty, tp_price)
                    engine.tp_confirmed = True
                    await log.ainfo("watchdog_tp_placed", symbol=sym, tp_price=tp_price, qty=qty)
                except Exception as e:
                    err_str = str(e)
                    await log.aerror("watchdog_tp_failed", symbol=sym, tp_price=tp_price, error=err_str)
                    if "-2021" in err_str:
                        await _watchdog_market_close(sym, exit_side, qty, "TP_PASSED")
                        continue
            elif not has_tp and not tp_price:
                await log.awarning("watchdog_tp_no_price", symbol=sym, msg="TP fiyati yok (webhook gelmemis)")

            if not has_sl and sl_needed and sl_price:
                try:
                    await place_stop_market_order(sym, exit_side, qty, sl_price)
                    engine.sl_confirmed = True
                    await log.ainfo("watchdog_sl_placed", symbol=sym, sl_price=sl_price, qty=qty)
                except Exception as e:
                    err_str = str(e)
                    await log.aerror("watchdog_sl_failed", symbol=sym, sl_price=sl_price, error=err_str)
                    if "-2021" in err_str:
                        await _watchdog_market_close(sym, exit_side, qty, "SL_PASSED")
                        continue
            elif not has_sl and sl_needed and not sl_price:
                await log.awarning("watchdog_sl_no_price", symbol=sym, msg="SL fiyati yok (webhook gelmemis)")

            if not has_tp or (not has_sl and sl_needed):
                await log.awarning(
                    "watchdog_tpsl_missing",
                    symbol=sym,
                    side=engine.position_side,
                    entry=entry_price,
                    tp_missing=not has_tp,
                    sl_missing=not has_sl and sl_needed,
                )

        except Exception as e:
            await log.aerror("watchdog_error", symbol=sym, error=str(e))


async def _engine_loop() -> None:
    """Ana dongu — tum aktif engine'leri fiyat tick'leriyle besler."""
    from app.modules.st_signal_logger import log_st_signal
    from datetime import datetime, timezone, timedelta

    tz_ist = timezone(timedelta(hours=3))

    await log.ainfo("signal_engine_loop_starting")

    # Warmup bekle — price_stream hazir olsun
    await asyncio.sleep(8)

    # Engine'leri olustur
    try:
        all_settings = await get_all_settings()
        await log.ainfo("signal_engine_settings_loaded", count=len(all_settings),
                        symbols=[s.get("symbol") for s in all_settings])
    except Exception as e:
        await log.aerror("signal_engine_settings_error", error=str(e))
        return

    # ha_enabled semboller HA motorunda calisir — normal motordan haric tut
    active_symbols = [s for s in all_settings if s.get("active") and s.get("listening") and not s.get("ha_enabled")]
    await log.ainfo("signal_engine_active_symbols",
                     count=len(active_symbols),
                     symbols=[s.get("symbol") for s in active_symbols])
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

    # Startup: orphan limit emirleri temizle (watchdog DEVRE DISI — webhook modu)
    await _cleanup_orphan_limit_orders()
    # _tpsl_watchdog() — DEVRE DISI: TP/SL sadece webhook'tan gelir
    await log.ainfo("signal_engine_startup_done_no_watchdog")

    # Pozisyon sync periyodik (30sn)
    last_pos_sync = time.time()
    POS_SYNC_INTERVAL = 30

    # Fill takip periyodik (2sn) — pending order varsa Binance'tan kontrol
    last_fill_check = time.time()
    FILL_CHECK_INTERVAL = 2

    # TP/SL watchdog periyodik (10sn) — yedek guevenlik
    last_tpsl_check = time.time()
    TPSL_CHECK_INTERVAL = 10

    # Settings reload periyodik (60sn)
    last_settings_reload = time.time()
    SETTINGS_RELOAD_INTERVAL = 60

    # Ana dongu — her 200ms fiyat kontrol
    while True:
        try:
            await asyncio.sleep(0.2)

            now = time.time()

            # Fill takip + pozisyon kapanma kontrolu (2sn)
            if now - last_fill_check > FILL_CHECK_INTERVAL:
                last_fill_check = now
                for engine in _engines.values():
                    if engine.trade_pending:
                        await engine.check_pending_fill()
                    elif engine.has_position:
                        await engine.check_position_closed()

            # Periyodik pozisyon sync (30sn)
            if now - last_pos_sync > POS_SYNC_INTERVAL:
                last_pos_sync = now
                for engine in _engines.values():
                    await engine._sync_position()

            # TP/SL watchdog DEVRE DISI — webhook modunda sisteme mudahale etmesin
            # TP/SL sadece webhook'tan (Pine Script) gelir, watchdog karismasin
            # if now - last_tpsl_check > TPSL_CHECK_INTERVAL:
            #     last_tpsl_check = now
            #     await _tpsl_watchdog()

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
                    # Silinen/deaktif/ha_enabled sembolleri kaldir
                    active_syms = {s["symbol"] for s in fresh if s.get("active") and s.get("listening") and not s.get("ha_enabled")}
                    for sym in list(_engines.keys()):
                        if sym not in active_syms:
                            del _engines[sym]
                            await log.ainfo("signal_engine_removed", symbol=sym)
                    # Periyodik orphan limit temizligi (settings reload ile birlikte, 60sn)
                    await _cleanup_orphan_limit_orders()
                    # used_a eski kayitlari temizle (24 saatten eski)
                    for eng in _engines.values():
                        eng._cleanup_used_a()
                except Exception as e:
                    await log.awarning("signal_engine_settings_reload_error", error=str(e))

            # Her engine icin fiyat tick
            for sym, engine in _engines.items():
                price = get_live_price(sym)
                if price is None:
                    continue

                signal = await engine.on_price_tick(price)
                if signal:
                    # Sinyal bulundu — HER ZAMAN logla (pozisyon olsa bile)
                    dt_str = datetime.now(tz_ist).strftime("%Y-%m-%d %H:%M:%S")

                    # used_a'ya ekle — ayni A mumundan tekrar sinyal uretilmesin
                    a_time = signal.get("candle_a_time")
                    if a_time:
                        async with engine._state_lock:
                            engine.used_a.add(a_time)
                            engine._save_used_a()

                    await log.ainfo(
                        "signal_engine_signal",
                        symbol=sym,
                        direction=signal["direction"],
                        entry_price=signal["entry_price"],
                        rsi_a=signal["rsi_a"],
                        rsi_b=signal["rsi_b"],
                        gap=signal["gap"],
                        has_position=engine.has_position,
                        mode="bypass",
                    )

                    # Signal log — motor bypass: entered=0, skip_reason=motor_bypass
                    await log_st_signal(
                        dt=dt_str,
                        symbol=sym,
                        direction=signal["direction"],
                        band=engine.interval,
                        price=signal["entry_price"],
                        entered=False,
                        source="server",
                        rsi_a=signal["rsi_a"],
                        rsi_b=signal["rsi_b"],
                        gap=signal["gap"],
                        candle_a_time=signal.get("candle_a_time"),
                        skip_reason="motor_bypass",
                    )

                    # MOTOR ISLEM ACMIYOR — sadece sinyal log'u + analiz icin
                    # Tum trade acma TradingView webhook (st_webhook.py) uzerinden
                    # SL/TP placement, fill takibi, pozisyon yonetimi yine motorda

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
