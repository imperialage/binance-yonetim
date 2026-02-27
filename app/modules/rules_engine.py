"""Deterministic rules engine: scoring, bias, veto, decision."""

from __future__ import annotations

from app.schemas.config import RuntimeConfig
from app.schemas.evaluation import AggregationResult, RulesOutput
from app.utils.logging import get_logger

log = get_logger(__name__)

_DIRECTION = {
    "BUY": 1.0,
    "SELL": -1.0,
    "CLOSE": 0.0,
    "NEUTRAL": 0.0,
}


def evaluate(aggregation: AggregationResult, config: RuntimeConfig) -> RulesOutput:
    """Run deterministic rules on aggregated data."""

    score = 0.0
    reasons: list[str] = []

    for tf, summary in aggregation.timeframes.items():
        tf_weight = config.tf_weights.get(tf, 0.0)

        for ind_sig in summary.indicators:
            ind_weight = config.indicator_weights.get(ind_sig.indicator, 1.0)
            direction = _DIRECTION.get(ind_sig.signal.upper(), 0.0)
            contribution = direction * tf_weight * ind_weight * ind_sig.strength
            score += contribution

            if direction != 0.0:
                reasons.append(
                    f"{ind_sig.indicator}@{tf}: {ind_sig.signal} "
                    f"(str={ind_sig.strength:.1f}, contrib={contribution:+.3f})"
                )

    threshold = config.threshold

    # Determine bias
    if score >= threshold:
        bias = "LONG"
    elif score <= -threshold:
        bias = "SHORT"
    else:
        bias = "NEUTRAL"

    # Veto logic: check 4h alignment
    veto_applied = False
    veto_reason: str | None = None

    tf_4h = aggregation.timeframes.get("4h")
    if tf_4h is not None:
        # Compute 4h directional score
        h4_score = 0.0
        h4_weight = config.tf_weights.get("4h", 0.5)
        for ind_sig in tf_4h.indicators:
            ind_w = config.indicator_weights.get(ind_sig.indicator, 1.0)
            d = _DIRECTION.get(ind_sig.signal.upper(), 0.0)
            h4_score += d * h4_weight * ind_w * ind_sig.strength

        h4_net_sell = tf_4h.sell_count > tf_4h.buy_count or h4_score < 0
        h4_net_buy = tf_4h.buy_count > tf_4h.sell_count or h4_score > 0

        if bias == "LONG" and h4_net_sell:
            veto_applied = True
            veto_reason = "4H net SELL — LONG_SETUP vetoed"
        elif bias == "SHORT" and h4_net_buy:
            veto_applied = True
            veto_reason = "4H net BUY — SHORT_SETUP vetoed"

    # Decision mapping
    if veto_applied:
        decision = "NO_TRADE"
    elif bias == "LONG":
        decision = "LONG_SETUP"
    elif bias == "SHORT":
        decision = "SHORT_SETUP"
    else:
        decision = "WATCH"

    # Confidence: min(100, abs(score) / (threshold * 2) * 100)
    confidence = min(100, int(abs(score) / (threshold * 2) * 100))

    return RulesOutput(
        symbol=aggregation.symbol,
        decision=decision,
        bias=bias,
        confidence=confidence,
        score=round(score, 4),
        threshold=threshold,
        reasons=reasons,
        veto_applied=veto_applied,
        veto_reason=veto_reason,
    )
