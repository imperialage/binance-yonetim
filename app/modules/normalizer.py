"""Normalize raw TradingView payloads into canonical NormalizedEvent.

TradingView indicator alerts (study, NOT strategy) send all placeholder
values as strings.  This module handles:
  - symbol prefix removal  ("BINANCE:ETHUSDT" → "ETHUSDT")
  - tf normalization        ("60" → "1h", "4H" → "4h")
  - signal strict check     (only BUY / SELL accepted from TV indicators)
  - ts / price string→number parsing
"""

from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime, timezone

from app.schemas.webhook import NormalizedEvent, SignalType, WebhookPayload
from app.utils.logging import get_logger

log = get_logger(__name__)

# ── Signal map ───────────────────────────────────────
# For indicator (study) alerts only BUY and SELL are expected.
# We still keep CLOSE / NEUTRAL for manual / future use.
_SIGNAL_MAP: dict[str, SignalType] = {
    "BUY": SignalType.BUY,
    "SELL": SignalType.SELL,
    "CLOSE": SignalType.CLOSE,
    "NEUTRAL": SignalType.NEUTRAL,
    "LONG": SignalType.BUY,
    "SHORT": SignalType.SELL,
    "EXIT": SignalType.CLOSE,
    "FLAT": SignalType.NEUTRAL,
}

# Signals accepted from TradingView indicator alerts
_STRICT_SIGNALS = {"BUY", "SELL", "LONG", "SHORT"}

# ── Timeframe normalization map ──────────────────────
_TF_MAP: dict[str, str] = {
    "5": "5m",
    "5m": "5m",
    "15": "15m",
    "15m": "15m",
    "60": "1h",
    "1h": "1h",
    "1H": "1h",
    "240": "4h",
    "4h": "4h",
    "4H": "4h",
}

VALID_TF = {"5m", "15m", "1h", "4h"}

# ── Symbol prefix pattern ────────────────────────────
# "BINANCE:ETHUSDT.P" → "ETHUSDT"
_EXCHANGE_PREFIX_RE = re.compile(r"^[A-Z0-9]+:")
_SUFFIX_RE = re.compile(r"\.[A-Z]+$")  # ".P" suffix on perps


def normalize_symbol(raw: str) -> str:
    """Strip exchange prefix and contract suffix, return uppercase symbol."""
    s = raw.strip().upper()
    s = _EXCHANGE_PREFIX_RE.sub("", s)  # remove BINANCE: etc.
    s = _SUFFIX_RE.sub("", s)  # remove .P suffix
    return s


def normalize_tf(raw_tf: str) -> str | None:
    """Normalize a timeframe string.  Returns canonical form or None if invalid."""
    cleaned = raw_tf.strip()
    result = _TF_MAP.get(cleaned)
    if result:
        return result
    result = _TF_MAP.get(cleaned.lower())
    if result:
        return result
    return None


def _safe_int(value: int | str | None) -> int | None:
    """Parse int from string or ISO datetime; return None on failure."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        # Try plain integer first
        try:
            return int(value)
        except ValueError:
            pass
        # Try ISO datetime formats (TradingView {{timenow}} can send these)
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(value.strip(), fmt).replace(tzinfo=timezone.utc)
                return int(dt.timestamp())
            except ValueError:
                continue
    return None


def _safe_float(value: float | str | None) -> float | None:
    """Parse float from string; return None on failure."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _deterministic_hash(payload: WebhookPayload) -> str:
    """Generate deterministic event ID from payload content (includes price for uniqueness)."""
    key_parts = (
        f"{payload.indicator}:{payload.symbol}:{payload.tf}:"
        f"{payload.signal}:{payload.ts or ''}:{payload.price or ''}"
    )
    return hashlib.sha256(key_parts.encode()).hexdigest()[:16]


class NormalizeError:
    """Carries a 400-level error message back to the router."""

    def __init__(self, detail: str) -> None:
        self.detail = detail


def normalize(
    payload: WebhookPayload,
    fallback_price: float = 0.0,
    strict_signal: bool = True,
) -> NormalizedEvent | NormalizeError:
    """Normalize a webhook payload.

    Returns ``NormalizedEvent`` on success or ``NormalizeError`` with a
    human-readable detail string on validation failure (caller returns 400).
    """
    now = int(time.time())

    # ── Signal ───────────────────────────────────────
    raw_signal = payload.signal.strip().upper()
    if strict_signal and raw_signal not in _STRICT_SIGNALS:
        return NormalizeError(f"Invalid signal: '{payload.signal}'. Expected BUY or SELL.")
    signal = _SIGNAL_MAP.get(raw_signal)
    if signal is None:
        return NormalizeError(f"Unknown signal: '{payload.signal}'")

    # ── Timeframe ────────────────────────────────────
    tf = normalize_tf(payload.tf)
    if tf is None:
        return NormalizeError(f"Invalid timeframe: '{payload.tf}'")

    # ── Symbol ───────────────────────────────────────
    symbol = normalize_symbol(payload.symbol)
    if not symbol:
        return NormalizeError("Empty symbol after normalization")

    # ── ts ───────────────────────────────────────────
    parsed_ts = _safe_int(payload.ts)
    if payload.ts is not None and parsed_ts is None:
        return NormalizeError(f"Cannot parse ts as integer: '{payload.ts}'")
    ts = parsed_ts or now

    # ── price ────────────────────────────────────────
    parsed_price = _safe_float(payload.price)
    if payload.price is not None and parsed_price is None:
        return NormalizeError(f"Cannot parse price as number: '{payload.price}'")
    price = parsed_price if parsed_price is not None else fallback_price

    # ── strength ─────────────────────────────────────
    parsed_str = _safe_float(payload.strength)
    strength = max(0.0, min(1.0, parsed_str)) if parsed_str is not None else 0.5

    # ── event_id ─────────────────────────────────────
    event_id = payload.event_id or _deterministic_hash(payload)

    # ── raw (secret removed) ─────────────────────────
    raw_data = payload.model_dump()
    raw_data.pop("secret", None)

    return NormalizedEvent(
        event_id=event_id,
        received_at=now,
        ts=ts,
        indicator=payload.indicator.strip(),
        symbol=symbol,
        tf=tf,
        signal=signal,
        strength=strength,
        price=price,
        raw=raw_data,
    )
