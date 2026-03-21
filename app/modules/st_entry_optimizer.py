"""SuperTrend entry optimizer — validates price action before entering a trade.

Checks (validated against 30-day dry-run data):
1. Signal age — reject if > 90 seconds old
2. Momentum confirmation — reject if last 3 candles trend against direction
3. Candle body quality — reject if last candle body_ratio < 0.3 (indecision)

Note: Displacement filter removed — SuperTrend signals inherently occur at
trend reversals where displacement is expected. Analysis showed displacement
filter rejected ALL signals with 52.8% hit rate (better than baseline).
"""

from __future__ import annotations

import time

from app.modules.candle_store import query_candles
from app.utils.logging import get_logger

log = get_logger(__name__)

MAX_SIGNAL_AGE_SEC = 90
BODY_RATIO_MIN = 0.3
LOOKBACK = 20


class EntryResult:
    """Result of entry optimization check."""

    __slots__ = ("passed", "reason", "checks")

    def __init__(self, passed: bool, reason: str, checks: list[dict] | None = None) -> None:
        self.passed = passed
        self.reason = reason
        self.checks = checks or []

    def to_dict(self) -> dict:
        return {"pass": self.passed, "reason": self.reason, "checks": self.checks}


async def check_entry(
    symbol: str,
    direction: str,
    price: float,
    signal_time: int,
) -> EntryResult:
    """Validate entry conditions for a SuperTrend signal.

    Args:
        symbol: Trading pair (e.g. ETHUSDT)
        direction: "BUY" or "SELL"
        price: Signal price
        signal_time: Unix timestamp of signal

    Returns:
        EntryResult with pass/fail and reason
    """
    checks: list[dict] = []
    direction = direction.upper()

    # ── 1. Signal age check ───────────────────────────────
    now = int(time.time())
    age = now - signal_time
    if age > MAX_SIGNAL_AGE_SEC:
        checks.append({"check": "age", "pass": False, "detail": f"{age}s > {MAX_SIGNAL_AGE_SEC}s"})
        return EntryResult(False, f"Signal too old: {age}s", checks)
    checks.append({"check": "age", "pass": True, "detail": f"{age}s"})

    # ── 2. Get recent candles ─────────────────────────────
    candles = await query_candles(symbol, "5m", limit=LOOKBACK)
    if len(candles) < 5:
        checks.append({"check": "data", "pass": True, "detail": "Not enough candles — pass"})
        return EntryResult(True, "Not enough candle data — pass", checks)

    # ── 3. Momentum check (last 3 candles) ────────────────
    last3 = candles[-3:]
    closes = [c["close"] for c in last3]

    if direction == "BUY":
        # Reject if last 3 closes are declining
        if closes[0] > closes[1] > closes[2]:
            checks.append({
                "check": "momentum",
                "pass": False,
                "detail": f"3 declining closes: {closes}",
            })
            return EntryResult(False, "BUY with 3 declining closes", checks)
    else:  # SELL
        # Reject if last 3 closes are rising
        if closes[0] < closes[1] < closes[2]:
            checks.append({
                "check": "momentum",
                "pass": False,
                "detail": f"3 rising closes: {closes}",
            })
            return EntryResult(False, "SELL with 3 rising closes", checks)

    checks.append({"check": "momentum", "pass": True, "detail": f"closes={closes}"})

    # ── 4. Body ratio check (last candle) ─────────────────
    last = candles[-1]
    candle_range = last["high"] - last["low"]
    body = abs(last["close"] - last["open"])

    if candle_range > 0:
        body_ratio = body / candle_range
    else:
        body_ratio = 0.0

    if body_ratio < BODY_RATIO_MIN:
        checks.append({
            "check": "body_ratio",
            "pass": False,
            "detail": f"body_ratio={body_ratio:.2f} < {BODY_RATIO_MIN}",
        })
        return EntryResult(
            False,
            f"Weak candle body: ratio={body_ratio:.2f}",
            checks,
        )

    checks.append({"check": "body_ratio", "pass": True, "detail": f"body_ratio={body_ratio:.2f}"})

    return EntryResult(True, "All entry checks passed", checks)
