"""Live RSI tracker — per-symbol RSI state in memory.

Startup'ta Binance'tan 200 mum ceker (warmup).
Her fiyat tick'inde RSI hesaplar.
Mum kapanisinda state ilerletir.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from app.utils.logging import get_logger

log = get_logger(__name__)

BINANCE_URL = "https://fapi.binance.com/fapi/v1/klines"

INTERVAL_SECONDS = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "1d": 86400,
}

# Aktif tracker'lar: key = "SYMBOL_INTERVAL"
_trackers: dict[str, RSITracker] = {}


class RSITracker:
    """Tek sembol + interval icin canli RSI takibi."""

    def __init__(self, symbol: str, interval: str, length: int = 10):
        self.symbol = symbol
        self.interval = interval
        self.length = length
        self.closes: list[float] = []  # son N mum close'lari
        self.avg_gain: float = 0.0
        self.avg_loss: float = 0.0
        self.prev_close: float = 0.0
        self.current_rsi: float | None = None
        self.last_candle_time: int = 0  # son kapanmis mumun zamani
        self.warmed_up: bool = False

    async def warmup(self) -> None:
        """Binance'tan TUM gecmis mumlari cekip RSI state olustur (sayfalama ile)."""
        try:
            proxy_url = None
            try:
                from app.config import settings
                proxy_url = settings.binance_proxy_url or None
            except Exception:
                pass

            import asyncio

            # Sembolun ilk mumundan baslayarak tum verileri cek
            all_klines = []
            # 1 Ocak 2026'dan basla (semboller bu tarihten sonra listelenmis)
            current_start = 1735689600000  # 2026-01-01 UTC ms
            end_ms = int(time.time() * 1000)
            iv_ms = INTERVAL_SECONDS.get(self.interval, 900) * 1000

            async with httpx.AsyncClient(timeout=15, proxy=proxy_url) as client:
                while current_start < end_ms:
                    resp = await client.get(BINANCE_URL, params={
                        "symbol": self.symbol,
                        "interval": self.interval,
                        "startTime": current_start,
                        "endTime": end_ms,
                        "limit": 1500,
                    })
                    resp.raise_for_status()
                    batch = resp.json()
                    if not batch:
                        break
                    all_klines.extend(batch)
                    current_start = int(batch[-1][0]) + iv_ms
                    if len(batch) < 1500:
                        break
                    await asyncio.sleep(0.2)

            klines = all_klines
            if not klines or len(klines) < self.length + 2:
                return

            self.closes = [float(k[4]) for k in klines]
            n = len(self.closes)

            # Son mumun zamani
            self.last_candle_time = int(klines[-1][0]) // 1000

            # Wilder's RMA — tam hesaplama
            gains = [0.0] * n
            losses = [0.0] * n
            for i in range(1, n):
                d = self.closes[i] - self.closes[i - 1]
                gains[i] = max(d, 0.0)
                losses[i] = max(-d, 0.0)

            # SMA baslangic
            self.avg_gain = sum(gains[1:self.length + 1]) / self.length
            self.avg_loss = sum(losses[1:self.length + 1]) / self.length

            # RMA sondan bir onceki muma kadar ilerlet
            for i in range(self.length + 1, n - 1):
                self.avg_gain = (self.avg_gain * (self.length - 1) + gains[i]) / self.length
                self.avg_loss = (self.avg_loss * (self.length - 1) + losses[i]) / self.length

            # prev_close = sondan bir onceki mum (state bu noktada)
            self.prev_close = self.closes[-2] if len(self.closes) >= 2 else self.closes[-1]

            # Son mumun RSI'ini hesapla
            self.current_rsi = self._calc_rsi(self.closes[-1])

            self.warmed_up = True
            await log.ainfo(
                "live_rsi_warmup_done",
                symbol=self.symbol,
                interval=self.interval,
                candles=n,
                rsi=self.current_rsi,
            )

        except Exception as e:
            await log.aerror("live_rsi_warmup_error", symbol=self.symbol, error=str(e))

    def _calc_rsi(self, current_close: float) -> float:
        """Mevcut state'ten RSI hesapla (state degistirmez)."""
        d = current_close - self.prev_close
        g = max(d, 0.0)
        l_val = max(-d, 0.0)
        ag = (self.avg_gain * (self.length - 1) + g) / self.length
        al = (self.avg_loss * (self.length - 1) + l_val) / self.length
        if al == 0:
            return 100.0
        return round(100.0 - (100.0 / (1.0 + ag / al)), 2)

    def update_price(self, price: float) -> float | None:
        """Yeni fiyat geldi — RSI hesapla (state degistirmez)."""
        if not self.warmed_up:
            return None
        self.current_rsi = self._calc_rsi(price)
        return self.current_rsi

    def advance_candle(self, closed_price: float) -> None:
        """Mum kapandi — state ilerlet, yeni mum icin hazirlan."""
        if not self.warmed_up:
            return

        d = closed_price - self.prev_close
        g = max(d, 0.0)
        l_val = max(-d, 0.0)

        self.avg_gain = (self.avg_gain * (self.length - 1) + g) / self.length
        self.avg_loss = (self.avg_loss * (self.length - 1) + l_val) / self.length
        self.prev_close = closed_price

        # closes listesini guncelle (son 200 tut)
        self.closes.append(closed_price)
        if len(self.closes) > 200:
            self.closes.pop(0)

        self.last_candle_time = int(time.time())

    def should_advance(self) -> bool:
        """Mum kapanma zamani geldi mi?"""
        iv_sec = INTERVAL_SECONDS.get(self.interval, 900)
        now = int(time.time())
        candle_start = (now // iv_sec) * iv_sec
        # Son kapanmis mumun zamani candle_start'tan eskiyse yeni mum kapanmis
        return candle_start > self.last_candle_time and now - candle_start > 2


async def get_or_create_tracker(symbol: str, interval: str, rsi_len: int = 10) -> RSITracker:
    """Tracker al veya yenisini olustur + warmup yap."""
    key = f"{symbol.upper()}_{interval}"
    if key not in _trackers:
        tracker = RSITracker(symbol.upper(), interval, rsi_len)
        await tracker.warmup()
        _trackers[key] = tracker
    return _trackers[key]


async def get_live_rsi(symbol: str, interval: str, price: float, rsi_len: int = 10) -> float | None:
    """Canli RSI dondur. Tracker yoksa olusturur."""
    tracker = await get_or_create_tracker(symbol, interval, rsi_len)

    # Mum kapanma kontrolu
    if tracker.should_advance():
        # Son fiyati mum kapanisi olarak kullan ve state ilerlet
        tracker.advance_candle(price)
        await log.ainfo("live_rsi_candle_advanced", symbol=symbol, interval=interval, close=price)

    return tracker.update_price(price)


def get_cached_rsi(symbol: str, interval: str) -> float | None:
    """Son hesaplanmis RSI'i dondur (warmup yoksa None)."""
    key = f"{symbol.upper()}_{interval}"
    tracker = _trackers.get(key)
    if tracker and tracker.warmed_up:
        return tracker.current_rsi
    return None
