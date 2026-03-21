"""Data-driven SuperTrend signal filter engine.

Filters based on analysis of 212K candles / 5249 signals:
- LOW band signals: 26.3% hit rate (worst) → reject
- UTC 07-08, 10-12 hours: worst performing → reject
- vol_ratio < 0.5 (20-candle window): 27.5% hit rate → reject
- Funding rate extremes: contrarian crowd risk → reject
- Historical stats: direction+band+hour success_rate < 0.55 → reject
"""

from __future__ import annotations

import statistics
import time
from datetime import datetime, timezone

from app.modules.candle_store import query_candles
from app.modules.st_signal_logger import get_signal_stats
from app.utils.logging import get_logger

log = get_logger(__name__)

VOL_MA_WINDOW = 20
VOL_RATIO_MIN = 0.5
FUNDING_THRESHOLD = 0.01
STATS_MIN_RATE = 0.55
STATS_MIN_SAMPLES = 30

# Hours with worst hit rates from analysis (UTC)
_BAD_HOURS = {7, 8, 10, 11, 12}


class FilterResult:
    """Result of a single filter check."""

    __slots__ = ("passed", "reason", "name")

    def __init__(self, passed: bool, reason: str, name: str = "") -> None:
        self.passed = passed
        self.reason = reason
        self.name = name

    def to_dict(self) -> dict:
        return {"pass": self.passed, "reason": self.reason, "name": self.name}


# ── Individual filters ────────────────────────────────────


def filter_hour(signal_time: datetime) -> FilterResult:
    """UTC 07:00-08:59, 10:00-12:59 → reject."""
    hour = signal_time.hour
    if hour in _BAD_HOURS:
        return FilterResult(
            False,
            f"Bad hour UTC {hour:02d}:00 (hit rate <25%)",
            "hour",
        )
    return FilterResult(True, "Hour OK", "hour")


def filter_band(band: str) -> FilterResult:
    """band == LOW → reject (26.3% hit rate vs HIGH 43.5%)."""
    if band.upper() == "LOW":
        return FilterResult(False, "LOW band — 26.3% hit rate", "band")
    return FilterResult(True, f"Band {band} OK", "band")


async def filter_volume(symbol: str, interval: str = "5m") -> FilterResult:
    """vol_ratio < 0.5 (20-candle MA) → reject low-volume signals."""
    candles = await query_candles(symbol, interval, limit=VOL_MA_WINDOW + 1)

    if len(candles) < VOL_MA_WINDOW + 1:
        return FilterResult(True, "Not enough candles for vol check — pass", "volume")

    # Latest candle is the signal candle
    current_vol = candles[-1]["volume"]
    prev_vols = [c["volume"] for c in candles[:-1]]

    vol_ma = statistics.mean(prev_vols)
    if vol_ma <= 0:
        return FilterResult(True, "Vol MA is zero — pass", "volume")

    vol_ratio = current_vol / vol_ma

    if vol_ratio < VOL_RATIO_MIN:
        return FilterResult(
            False,
            f"Low volume: vol_ratio={vol_ratio:.2f} < {VOL_RATIO_MIN}",
            "volume",
        )
    return FilterResult(True, f"Volume OK: vol_ratio={vol_ratio:.2f}", "volume")


async def filter_funding(symbol: str, direction: str) -> FilterResult:
    """BUY + funding > 0.01 → reject, SELL + funding < -0.01 → reject."""
    try:
        from app.modules.binance_client import get_funding_rate

        rate = await get_funding_rate(symbol)
        if rate is None:
            return FilterResult(True, "Funding rate unavailable — pass", "funding")

        if direction.upper() == "BUY" and rate > FUNDING_THRESHOLD:
            return FilterResult(
                False,
                f"BUY + high funding ({rate:.4f} > {FUNDING_THRESHOLD})",
                "funding",
            )
        if direction.upper() == "SELL" and rate < -FUNDING_THRESHOLD:
            return FilterResult(
                False,
                f"SELL + negative funding ({rate:.4f} < -{FUNDING_THRESHOLD})",
                "funding",
            )
        return FilterResult(True, f"Funding OK: {rate:.4f}", "funding")
    except Exception as e:
        log.warning("funding_filter_error", error=str(e))
        return FilterResult(True, f"Funding check failed — pass ({e})", "funding")


async def filter_stats(direction: str, band: str, hour: int) -> FilterResult:
    """Historical success_rate < 0.55 with sample_count >= 30 → reject."""
    stats = await get_signal_stats(direction, band, hour)

    if stats is None:
        return FilterResult(True, "No historical stats yet — pass", "stats")

    sample_count = stats["sample_count"]
    success_rate = stats["success_rate"]

    if sample_count < STATS_MIN_SAMPLES:
        return FilterResult(
            True,
            f"Not enough samples ({sample_count} < {STATS_MIN_SAMPLES}) — pass",
            "stats",
        )

    if success_rate < STATS_MIN_RATE:
        return FilterResult(
            False,
            f"Low success rate: {success_rate:.2f} < {STATS_MIN_RATE} "
            f"(n={sample_count}, {direction}/{band}/h{hour})",
            "stats",
        )

    return FilterResult(
        True,
        f"Stats OK: {success_rate:.2f} (n={sample_count})",
        "stats",
    )


# ── Main filter runner ────────────────────────────────────


async def run_filters(
    symbol: str,
    direction: str,
    band: str,
    price: float,
    signal_time: datetime,
) -> tuple[bool, list[dict], float | None]:
    """Run all filters in sequence. Returns (passed, filter_results, vol_ratio).

    Stops at the first rejection (short-circuit).
    """
    results: list[dict] = []
    vol_ratio: float | None = None

    # 1. Hour filter
    r = filter_hour(signal_time)
    results.append(r.to_dict())
    if not r.passed:
        return False, results, vol_ratio

    # 2. Band filter
    r = filter_band(band)
    results.append(r.to_dict())
    if not r.passed:
        return False, results, vol_ratio

    # 3. Volume filter (also captures vol_ratio for logging)
    candles = await query_candles(symbol, "5m", limit=VOL_MA_WINDOW + 1)
    if len(candles) >= VOL_MA_WINDOW + 1:
        current_vol = candles[-1]["volume"]
        prev_vols = [c["volume"] for c in candles[:-1]]
        vol_ma = statistics.mean(prev_vols)
        if vol_ma > 0:
            vol_ratio = round(current_vol / vol_ma, 4)

    r = await filter_volume(symbol)
    results.append(r.to_dict())
    if not r.passed:
        return False, results, vol_ratio

    # 4. Funding rate filter
    r = await filter_funding(symbol, direction)
    results.append(r.to_dict())
    if not r.passed:
        return False, results, vol_ratio

    # 5. Historical stats filter
    hour = signal_time.hour
    r = await filter_stats(direction, band, hour)
    results.append(r.to_dict())
    if not r.passed:
        return False, results, vol_ratio

    return True, results, vol_ratio
