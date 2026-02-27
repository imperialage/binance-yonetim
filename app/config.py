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


settings = Settings()  # type: ignore[call-arg]
