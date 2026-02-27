"""Webhook request & normalized event schemas."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class SignalType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    CLOSE = "CLOSE"
    NEUTRAL = "NEUTRAL"


class WebhookPayload(BaseModel):
    """Raw TradingView webhook payload.

    TradingView sends ALL template values as strings, so ts and price
    are accepted as ``str | int | float | None`` and parsed downstream.
    """

    secret: str
    indicator: str
    symbol: str
    tf: str
    signal: str
    strength: float | str | None = None
    price: float | str | None = None
    event_id: str | None = None
    ts: int | str | None = None

    model_config = {"extra": "allow"}


class NormalizedEvent(BaseModel):
    """Canonical internal event representation."""

    event_id: str
    received_at: int
    ts: int
    indicator: str
    symbol: str = Field(examples=["ETHUSDT"])
    tf: str = Field(examples=["15m", "1h", "4h"])
    signal: SignalType
    strength: float = Field(ge=0.0, le=1.0, default=0.5)
    price: float
    raw: dict[str, Any]


class WebhookResponse(BaseModel):
    status: str = "accepted"
    event_id: str
    decision: str | None = None
    bias: str | None = None
    confidence: int | None = None
    score: float | None = None
    message: str | None = None
