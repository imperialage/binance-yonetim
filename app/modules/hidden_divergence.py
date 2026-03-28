"""Hidden RSI Divergence sinyal tespiti — XAGUSDT icin.

Her mum kapanisinda son 13 mumu kontrol eder.
Bullish hidden divergence: fiyat dusuk dip + RSI yuksek dip → LONG
Bearish hidden divergence: fiyat yuksek tepe + RSI dusuk tepe → SHORT
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from app.modules.candle_store import query_candles
from app.utils.logging import get_logger

log = get_logger(__name__)

# ── Kullanilmis Mum A takibi (persistent) ──
_DATA_DIR = os.getenv("DATA_DIR", "data")
_USED_A_PATH = Path(_DATA_DIR) / "used_divergence_a.json"
_used_a_times: set[int] = set()


def _load_used_a() -> None:
    """Startup'ta kullanilmis A mumlarini diskten yukle."""
    global _used_a_times
    try:
        if _USED_A_PATH.exists():
            data = json.loads(_USED_A_PATH.read_text())
            _used_a_times = set(data)
    except Exception:
        _used_a_times = set()


def _save_used_a() -> None:
    """Kullanilmis A mumlarini diske kaydet."""
    try:
        _USED_A_PATH.parent.mkdir(parents=True, exist_ok=True)
        # Son 500 entry tut (eski olanlari temizle)
        trimmed = sorted(_used_a_times)[-500:]
        _USED_A_PATH.write_text(json.dumps(trimmed))
    except Exception as e:
        log.warning("used_a_save_failed", error=str(e))


def _mark_a_used(open_time: int) -> None:
    """Bir Mum A'yi kullanilmis olarak isaretle."""
    _used_a_times.add(open_time)
    _save_used_a()


# Startup'ta yukle
_load_used_a()


class DivergenceSignal:
    """Tespit edilen divergence sinyali."""

    __slots__ = (
        "direction", "entry_price", "candle_a", "candle_b",
        "rsi_a", "rsi_b", "gap",
    )

    def __init__(
        self,
        direction: str,
        entry_price: float,
        candle_a: dict,
        candle_b: dict,
        rsi_a: float,
        rsi_b: float,
        gap: int,
    ) -> None:
        self.direction = direction
        self.entry_price = entry_price
        self.candle_a = candle_a
        self.candle_b = candle_b
        self.rsi_a = rsi_a
        self.rsi_b = rsi_b
        self.gap = gap

    def to_dict(self) -> dict[str, Any]:
        return {
            "direction": self.direction,
            "entry_price": self.entry_price,
            "candle_a_time": self.candle_a.get("date", ""),
            "candle_b_time": self.candle_b.get("date", ""),
            "rsi_a": self.rsi_a,
            "rsi_b": self.rsi_b,
            "gap": self.gap,
            "low_a": self.candle_a.get("low"),
            "low_b": self.candle_b.get("low"),
            "high_a": self.candle_a.get("high"),
            "high_b": self.candle_b.get("high"),
        }


async def check_divergence(
    symbol: str,
    interval: str = "15m",
    rsi_long_threshold: float = 32.0,
    rsi_short_threshold: float = 70.0,
    max_gap: int = 12,
    entry_buffer: float = 0.001,
) -> DivergenceSignal | None:
    """Son 13 mumda hidden divergence kontrol et.

    Returns DivergenceSignal or None.
    """
    candles = await query_candles(symbol, interval, limit=13)

    if len(candles) < 3:
        return None

    # RSI degerleri kontrol
    for c in candles:
        if c.get("rsi_10") is None:
            return None  # RSI henuz hesaplanmamis

    n = len(candles)

    # ── SHORT kontrolu (Bearish Hidden Divergence) ──
    for a_idx in range(n - 1):
        a = candles[a_idx]
        a_time = a.get("open_time", 0)

        if a_time in _used_a_times:
            continue

        rsi_a = a["rsi_10"]
        if rsi_a is None or rsi_a < rsi_short_threshold:
            continue  # A mumunun RSI >= 70 olmali

        for b_idx in range(a_idx + 1, min(a_idx + max_gap + 1, n)):
            b = candles[b_idx]
            rsi_b = b.get("rsi_10")
            if rsi_b is None:
                continue

            if b["high"] > a["high"] and rsi_b < rsi_a:
                # Bearish hidden divergence bulundu
                entry_price = round(b["high"] * (1 - entry_buffer), 6)
                gap = b_idx - a_idx

                _mark_a_used(a_time)

                signal = DivergenceSignal(
                    direction="SELL",
                    entry_price=entry_price,
                    candle_a=a,
                    candle_b=b,
                    rsi_a=rsi_a,
                    rsi_b=rsi_b,
                    gap=gap,
                )

                await log.ainfo(
                    "divergence_detected",
                    symbol=symbol,
                    direction="SELL",
                    rsi_a=rsi_a,
                    rsi_b=rsi_b,
                    high_a=a["high"],
                    high_b=b["high"],
                    entry_price=entry_price,
                    gap=gap,
                    a_date=a.get("date"),
                    b_date=b.get("date"),
                )
                return signal

    # ── LONG kontrolu (Bullish Hidden Divergence) ──
    for a_idx in range(n - 1):
        a = candles[a_idx]
        a_time = a.get("open_time", 0)

        if a_time in _used_a_times:
            continue

        rsi_a = a["rsi_10"]
        if rsi_a is None or rsi_a > rsi_long_threshold:
            continue  # A mumunun RSI <= 32 olmali

        for b_idx in range(a_idx + 1, min(a_idx + max_gap + 1, n)):
            b = candles[b_idx]
            rsi_b = b.get("rsi_10")
            if rsi_b is None:
                continue

            if b["low"] < a["low"] and rsi_b > rsi_a:
                # Bullish hidden divergence bulundu
                entry_price = round(b["low"] * (1 + entry_buffer), 6)
                gap = b_idx - a_idx

                _mark_a_used(a_time)

                signal = DivergenceSignal(
                    direction="BUY",
                    entry_price=entry_price,
                    candle_a=a,
                    candle_b=b,
                    rsi_a=rsi_a,
                    rsi_b=rsi_b,
                    gap=gap,
                )

                await log.ainfo(
                    "divergence_detected",
                    symbol=symbol,
                    direction="BUY",
                    rsi_a=rsi_a,
                    rsi_b=rsi_b,
                    low_a=a["low"],
                    low_b=b["low"],
                    entry_price=entry_price,
                    gap=gap,
                    a_date=a.get("date"),
                    b_date=b.get("date"),
                )
                return signal

    return None
