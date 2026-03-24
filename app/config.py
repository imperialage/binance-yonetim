"""Application settings loaded from environment / .env file."""

from __future__ import annotations

from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# ── Per-symbol trading config ──────────────────────────────
# Filtre parametreleri + TP/SL + izin verilen yönler
SYMBOL_CONFIGS: dict[str, dict[str, Any]] = {
    "BTCUSDT": {
        "bad_hours": {4, 19, 20},
        "allowed_directions": {"SELL", "BUY"},
        "band_filter": "LOW_ONLY",
        "tp_pct": 0.023,   # %2.3
        "sl_pct": 0.022,   # %2.2
        "weight": 0.27,    # $370 → ~$100
    },
    "XRPUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"SELL"},
        "vol_min": 2.0,
        "tp_pct": 0.022,   # %2.2
        "sl_pct": 0.013,   # %1.3
        "weight": 0.18,    # $370 → ~$67
    },
    "AVAXUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"SELL"},
        "vol_min": 1.5,
        "tp_pct": 0.016,   # %1.6
        "sl_pct": 0.025,   # %2.5
        "weight": 0.16,    # $370 → ~$59
    },
    "DOGEUSDT": {
        "bad_hours": {1, 3, 7, 10, 17},
        "allowed_directions": {"SELL"},
        "tp_pct": 0.024,   # %2.4
        "sl_pct": 0.015,   # %1.5
        "weight": 0.16,    # $370 → ~$59
    },
    "ETHUSDT": {
        "bad_hours": {3, 4, 17, 21, 23},
        "allowed_directions": {"SELL"},
        "band_filter": "MID_ONLY",
        "tp_pct": 0.015,   # %1.5
        "sl_pct": 0.024,   # %2.4
        "weight": 0.13,    # $370 → ~$48
    },
    "SOLUSDT": {
        "bad_hours": {0, 3, 5, 13, 21},
        "allowed_directions": {"SELL", "BUY"},
        "band_filter": "HIGH_MID",
        "tp_pct": 0.018,   # %1.8
        "sl_pct": 0.013,   # %1.3
        "weight": 0.10,    # $370 → ~$37
    },
}

# Tanımsız semboller için fallback
_DEFAULT_SYMBOL_CONFIG: dict[str, Any] = {
    "bad_hours": {7, 8, 10, 11, 12},
    "allowed_directions": {"BUY", "SELL"},
    "vol_min": None,
    "band_filter": None,
    "tp_pct": 0.005,
    "sl_pct": 0.015,
    "weight": 0.10,
}


def get_symbol_config(symbol: str) -> dict[str, Any]:
    """Return trading config for a symbol."""
    return SYMBOL_CONFIGS.get(symbol.upper(), _DEFAULT_SYMBOL_CONFIG)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Secrets ──────────────────────────────────────
    tv_webhook_secret: str = Field(description="TradingView shared secret")
    admin_token: str = Field(description="Admin API token")

    # ── Redis ────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"

    # ── AI ───────────────────────────────────────────
    ai_provider: str = "dummy"  # "dummy" | "openai"
    ai_api_key: str = ""
    ai_model: str = "gpt-4o-mini"
    ai_base_url: str = "https://api.openai.com/v1"

    # ── App ──────────────────────────────────────────
    log_level: str = "INFO"
    log_json: bool = True
    app_env: str = "production"

    # ── Rate limit ───────────────────────────────────
    rate_limit_window_sec: int = 10
    rate_limit_max_events: int = 30

    # ── Data Collector ─────────────────────────────
    collector_symbols: str = "XRPUSDT,BTCUSDT,AVAXUSDT,DOGEUSDT,ETHUSDT,SOLUSDT"
    collector_interval: str = "5m"

    # ── Binance Futures Trading ─────────────────────
    binance_api_key: str = ""
    binance_api_secret: str = ""
    binance_testnet: bool = True       # Safety: default to testnet
    trading_enabled: bool = True       # Auto-trade active
    stop_loss_pct: float = 0.015      # 1.5% stop-loss (grid search optimal)
    take_profit_pct: float = 0.005    # 0.5% take-profit (grid search optimal)
    binance_proxy_url: str = ""       # SOCKS5 proxy for static IP
    trading_symbols: str = "XRPUSDT,BTCUSDT,AVAXUSDT,DOGEUSDT,ETHUSDT,SOLUSDT"  # Comma-separated whitelist

    # Per-timeframe strategy overrides
    trading_timeframes: str = "5m"      # Active TFs: "5m" or "1m" or "1m,5m"
    strategy_1m_sl_pct: float = 0.0035  # 1m: %0.35 stop-loss
    strategy_1m_tp_pct: float = 0.0033  # 1m: %0.33 take-profit
    strategy_5m_sl_pct: float = 0.015   # 5m: %1.5 stop-loss (grid search optimal)
    strategy_5m_tp_pct: float = 0.005   # 5m: %0.5 take-profit (grid search optimal)

    def get_strategy(self, tf: str) -> tuple[float, float]:
        """Return (sl_pct, tp_pct) for given timeframe."""
        if tf == "1m":
            return self.strategy_1m_sl_pct, self.strategy_1m_tp_pct
        if tf == "5m":
            return self.strategy_5m_sl_pct, self.strategy_5m_tp_pct
        return self.stop_loss_pct, self.take_profit_pct

    def is_tf_enabled(self, tf: str) -> bool:
        """Check if a timeframe is enabled for trading."""
        enabled = {t.strip() for t in self.trading_timeframes.split(",") if t.strip()}
        return tf in enabled


settings = Settings()  # type: ignore[call-arg]
