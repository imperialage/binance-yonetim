"""Application settings loaded from environment / .env file."""

from __future__ import annotations

from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# ── Per-symbol trading config ──────────────────────────────
# Filtre parametreleri + TP/SL + izin verilen yönler
SYMBOL_CONFIGS: dict[str, dict[str, Any]] = {
    "ETHUSDT": {
        "bad_hours": {7, 8, 10, 11, 12},
        "allowed_directions": {"SELL"},          # BUY zararda
        "tp_pct": 0.005,   # %0.5
        "sl_pct": 0.015,   # %1.5
        "weight": 0.050,   # EV-based capital allocation
    },
    "BTCUSDT": {
        "bad_hours": {7, 8, 9, 10, 11, 12},
        "allowed_directions": {"SELL", "BUY"},
        "tp_pct": 0.007,   # %0.7
        "sl_pct": 0.020,   # %2.0
        "weight": 0.093,
    },
    "DOGEUSDT": {
        "bad_hours": {2, 7, 10, 12, 14},
        "allowed_directions": {"SELL", "BUY"},
        "tp_pct": 0.007,   # %0.7
        "sl_pct": 0.025,   # %2.5
        "weight": 0.205,
    },
    "SOLUSDT": {
        "bad_hours": {0, 7, 8, 9, 13},
        "allowed_directions": {"SELL", "BUY"},
        "tp_pct": 0.010,   # %1.0
        "sl_pct": 0.025,   # %2.5
        "weight": 0.223,
    },
    "XRPUSDT": {
        "bad_hours": {5, 7, 8, 9, 13},
        "allowed_directions": {"SELL", "BUY"},
        "tp_pct": 0.010,   # %1.0
        "sl_pct": 0.025,   # %2.5
        "weight": 0.213,
    },
    "BNBUSDT": {
        "bad_hours": {0, 7, 10, 13, 23},
        "allowed_directions": {"SELL", "BUY"},
        "tp_pct": 0.007,   # %0.7
        "sl_pct": 0.025,   # %2.5
        "weight": 0.216,
    },
}

# Tanımsız semboller için fallback
_DEFAULT_SYMBOL_CONFIG: dict[str, Any] = {
    "bad_hours": {7, 8, 10, 11, 12},
    "allowed_directions": {"BUY", "SELL"},
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
    collector_symbols: str = "ETHUSDT,BTCUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT"
    collector_interval: str = "5m"

    # ── Binance Futures Trading ─────────────────────
    binance_api_key: str = ""
    binance_api_secret: str = ""
    binance_testnet: bool = True       # Safety: default to testnet
    trading_enabled: bool = True       # Auto-trade active
    stop_loss_pct: float = 0.015      # 1.5% stop-loss (grid search optimal)
    take_profit_pct: float = 0.005    # 0.5% take-profit (grid search optimal)
    binance_proxy_url: str = ""       # SOCKS5 proxy for static IP
    trading_symbols: str = "ETHUSDT,BTCUSDT,DOGEUSDT,SOLUSDT,XRPUSDT,BNBUSDT"  # Comma-separated whitelist

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
