"""Runtime configuration schema (stored in Redis)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class RuntimeConfig(BaseModel):
    """Runtime-adjustable parameters – persisted in Redis as tv:config."""

    # ── Watchlist & Scheduler ────────────────────────
    watchlist_symbols: list[str] = Field(
        default=["ETHUSDT", "BTCUSDT"],
        description="Symbols to refresh on schedule even without new alerts",
    )
    refresh_rules_seconds: int = Field(
        default=30,
        description="How often (seconds) to refresh rules evaluation",
    )
    refresh_ai_seconds: int = Field(
        default=120,
        description="How often (seconds) to refresh AI explanation",
    )

    # ── Event store ──────────────────────────────────
    events_max_per_symbol: int = Field(
        default=1000,
        description="Maximum events to keep per symbol (LTRIM)",
    )

    # ── Aggregation windows ──────────────────────────
    tf_windows: dict[str, int] = Field(
        default={"5m": 180, "15m": 300, "1h": 900, "4h": 1800},
        description="Aggregation window per tf in seconds",
    )

    # ── Scoring ──────────────────────────────────────
    tf_weights: dict[str, float] = Field(
        default={"4h": 0.45, "1h": 0.25, "15m": 0.18, "5m": 0.12},
        description="Timeframe weights for scoring",
    )
    indicator_weights: dict[str, float] = Field(
        default={"AdaptiveTrendFlow": 1.0},
        description="Indicator weight overrides (default 1.0)",
    )
    threshold: float = Field(
        default=0.25,
        description="Score threshold for bias determination",
    )
