"""Application settings loaded from environment / .env file."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    # ── Binance Futures Trading ─────────────────────
    binance_api_key: str = ""
    binance_api_secret: str = ""
    binance_testnet: bool = True       # Safety: default to testnet
    trading_enabled: bool = False      # Kill-switch: default off
    stop_loss_pct: float = 0.005      # 0.5% stop-loss (fallback)
    take_profit_pct: float = 0.005    # 0.5% take-profit (fallback)
    binance_proxy_url: str = ""       # SOCKS5 proxy for static IP
    trading_symbols: str = ""         # Comma-separated whitelist e.g. "ETHUSDT,BTCUSDT"

    # Per-timeframe strategy overrides
    trading_timeframes: str = "5m"      # Active TFs: "5m" or "1m" or "1m,5m"
    strategy_1m_sl_pct: float = 0.005   # 1m: %0.50 stop-loss
    strategy_1m_tp_pct: float = 0.0025  # 1m: %0.25 take-profit
    strategy_5m_sl_pct: float = 0.01    # 5m: %1.00 stop-loss
    strategy_5m_tp_pct: float = 0.005   # 5m: %0.50 take-profit

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
