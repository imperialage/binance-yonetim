"""Evaluation / rules output schemas."""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, Field


class IndicatorSignal(BaseModel):
    indicator: str
    signal: str
    strength: float
    ts: int


class TimeframeSummary(BaseModel):
    tf: str
    buy_count: int = 0
    sell_count: int = 0
    close_count: int = 0
    neutral_count: int = 0
    indicators: list[IndicatorSignal] = []


class AggregationResult(BaseModel):
    symbol: str
    timeframes: dict[str, TimeframeSummary]
    used_events: list[dict[str, Any]]
    aggregated_at: int


class RulesOutput(BaseModel):
    symbol: str
    decision: str  # LONG_SETUP / SHORT_SETUP / WATCH / NO_TRADE
    bias: str  # LONG / SHORT / NEUTRAL
    confidence: int  # 0-100
    score: float
    threshold: float
    reasons: list[str]
    veto_applied: bool = False
    veto_reason: str | None = None


class MarketSummary(BaseModel):
    tf: str
    last_price: float
    green_candles: int
    red_candles: int
    slope: float  # last close - first close over last 20 candles


# ── Two-layer /latest response ──────────────────────

class LatestRules(BaseModel):
    decision: str
    bias: str
    confidence: int
    score: float
    reasons: list[str]
    signals_used: list[IndicatorSignal] = []
    aggregated_counts: dict[str, dict[str, int]] = {}


class LatestAI(BaseModel):
    lines: list[str] = Field(default_factory=list, max_length=6)
    generated_at: int


class Evaluation(BaseModel):
    evaluation_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    symbol: str
    rules: RulesOutput
    aggregation: AggregationResult
    market_data: dict[str, MarketSummary]
    ai_explanation: str | None = None
    evaluated_at: int
    generated_at: int = 0


class LatestEvaluation(BaseModel):
    """Two-layer /latest payload: rules refresh fast, AI refreshes slow."""

    evaluation_id: str
    symbol: str
    latest_rules: LatestRules
    latest_ai: LatestAI | None = None
    market_summary: dict[str, MarketSummary] | None = None
    evaluated_at: int
