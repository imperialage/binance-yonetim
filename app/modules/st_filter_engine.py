"""Data-driven SuperTrend signal filter engine.

Per-symbol filters:
- Direction: allowed_directions whitelist
- Hour: bad_hours set → reject
- Band: band_filter per-symbol (LOW_ONLY, MID_ONLY, HIGH_MID) → reject
- Volume: vol_min per-symbol (fallback VOL_RATIO_MIN=0.5) → reject
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

# Default bad hours (fallback — prefer per-symbol config)
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


def filter_hour(signal_time: datetime, bad_hours: set[int] | None = None) -> FilterResult:
    """UTC bad hours → reject. Uses per-symbol bad_hours if provided."""
    hour = signal_time.hour
    hours = bad_hours if bad_hours is not None else _BAD_HOURS
    if hour in hours:
        return FilterResult(
            False,
            f"Bad hour UTC {hour:02d}:00 (hit rate <25%)",
            "hour",
        )
    return FilterResult(True, "Hour OK", "hour")


def filter_band(band: str, band_filter: str | None = None) -> FilterResult:
    """Per-symbol band filter.

    band_filter values:
    - None: all bands pass
    - 'LOW_ONLY': only LOW passes
    - 'MID_ONLY': only MID passes
    - 'HIGH_MID': only HIGH and MID pass (LOW rejected)
    """
    if band_filter is None:
        return FilterResult(True, f"Band {band} OK (no filter)", "band")

    b = band.upper()
    if band_filter == "LOW_ONLY" and b != "LOW":
        return FilterResult(False, f"Band {b} rejected — only LOW allowed", "band")
    if band_filter == "MID_ONLY" and b != "MID":
        return FilterResult(False, f"Band {b} rejected — only MID allowed", "band")
    if band_filter == "HIGH_MID" and b not in ("HIGH", "MID"):
        return FilterResult(False, f"Band {b} rejected — only HIGH/MID allowed", "band")

    return FilterResult(True, f"Band {b} OK ({band_filter})", "band")


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
    Uses per-symbol config for bad_hours and allowed_directions.
    """
    from app.config import get_symbol_config

    sym_cfg = get_symbol_config(symbol)
    results: list[dict] = []
    vol_ratio: float | None = None

    # 0. Direction filter (per-symbol allowed_directions)
    allowed = sym_cfg.get("allowed_directions", {"BUY", "SELL"})
    if direction.upper() not in allowed:
        results.append(FilterResult(False, f"{direction} not in allowed_directions {allowed}", "direction").to_dict())
        return False, results, vol_ratio

    # 1. Hour filter (per-symbol bad_hours)
    r = filter_hour(signal_time, bad_hours=sym_cfg.get("bad_hours"))
    results.append(r.to_dict())
    if not r.passed:
        return False, results, vol_ratio

    # 2. Band filter (per-symbol band_filter config)
    r = filter_band(band, band_filter=sym_cfg.get("band_filter"))
    results.append(r.to_dict())
    if not r.passed:
        return False, results, vol_ratio

    # 3. Volume filter (captures vol_ratio + per-symbol vol_min check)
    candles = await query_candles(symbol, "5m", limit=VOL_MA_WINDOW + 1)
    if len(candles) >= VOL_MA_WINDOW + 1:
        current_vol = candles[-1]["volume"]
        prev_vols = [c["volume"] for c in candles[:-1]]
        vol_ma = statistics.mean(prev_vols)
        if vol_ma > 0:
            vol_ratio = round(current_vol / vol_ma, 4)

    # Per-symbol vol_min overrides global VOL_RATIO_MIN
    vol_min = sym_cfg.get("vol_min")
    if vol_min is not None and vol_ratio is not None:
        if vol_ratio < vol_min:
            r = FilterResult(False, f"Low volume: vol_ratio={vol_ratio:.2f} < vol_min={vol_min}", "volume")
            results.append(r.to_dict())
            return False, results, vol_ratio
        r = FilterResult(True, f"Volume OK: vol_ratio={vol_ratio:.2f} >= vol_min={vol_min}", "volume")
        results.append(r.to_dict())
    else:
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
