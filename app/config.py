"""Application settings loaded from environment / .env file."""

from __future__ import annotations

from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# ── Per-symbol trading config ──────────────────────────────
# Filtre parametreleri + TP/SL + izin verilen yönler
SYMBOL_CONFIGS: dict[str, dict[str, Any]] = {
    "BTCUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"BUY", "SELL"},
        "tp_pct": 0.010,   # %1.0
        "sl_pct": 0.003,   # %0.3
        "weight": 0.15,
    },
    "XRPUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"BUY", "SELL"},
        "tp_pct": 0.010,   # %1.0
        "sl_pct": 0.003,   # %0.3
        "weight": 0.15,
    },
    "AVAXUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"BUY", "SELL"},
        "tp_pct": 0.010,   # %1.0
        "sl_pct": 0.003,   # %0.3
        "weight": 0.15,
    },
    "DOGEUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"BUY", "SELL"},
        "tp_pct": 0.010,   # %1.0
        "sl_pct": 0.003,   # %0.3
        "weight": 0.15,
    },
    "ETHUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"BUY", "SELL"},
        "tp_pct": 0.010,   # %1.0
        "sl_pct": 0.003,   # %0.3
        "weight": 0.15,
    },
    "SOLUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"BUY", "SELL"},
        "tp_pct": 0.010,   # %1.0
        "sl_pct": 0.003,   # %0.3
        "weight": 0.15,
    },
    "XAGUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"BUY", "SELL"},
        "tp_pct": 0.010,   # %1.0
        "sl_pct": 0.003,   # %0.3
        "weight": 0.10,
        "interval": "15m",
        "weekend_closed": True,         # Cuma 20:00 - Pazar 24:00 (TR) islem kapali
    },
    "MYXUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"BUY", "SELL"},
        "tp_pct": 0.010,   # %1.0
        "sl_pct": 0.003,   # %0.3
        "weight": 0.10,
        "interval": "15m",
    },
    "ZECUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"BUY", "SELL"},
        "tp_pct": 0.010,   # %1.0
        "sl_pct": 0.003,   # %0.3
        "weight": 0.03,
    },
    "1000PEPEUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"BUY", "SELL"},
        "tp_pct": 0.010,   # %1.0
        "sl_pct": 0.003,   # %0.3
        "weight": 0.03,
    },
    "DOTUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"BUY", "SELL"},
        "tp_pct": 0.010,
        "sl_pct": 0.003,
        "weight": 0.03,
    },
    "NEARUSDT": {
        "bad_hours": set(),
        "allowed_directions": {"BUY", "SELL"},
        "tp_pct": 0.010,
        "sl_pct": 0.003,
        "weight": 0.03,
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
    "sl_enabled": True,
    "reverse_signal": False,
}


# ── Runtime overrides (in-memory, updated via API) ─────────
# Keys: symbol → {tp_pct, sl_pct, weight, allowed_directions, enabled, listening}
_runtime_overrides: dict[str, dict[str, Any]] = {}


def get_symbol_config(symbol: str) -> dict[str, Any]:
    """Return trading config for a symbol (with runtime overrides merged)."""
    base = dict(SYMBOL_CONFIGS.get(symbol.upper(), _DEFAULT_SYMBOL_CONFIG))
    # Defaults for new fields
    base.setdefault("enabled", True)
    base.setdefault("listening", True)
    base.setdefault("sl_enabled", True)
    base.setdefault("reverse_signal", False)
    overrides = _runtime_overrides.get(symbol.upper())
    if overrides:
        base.update(overrides)
    return base


def update_symbol_config(symbol: str, updates: dict[str, Any]) -> dict[str, Any]:
    """Apply runtime overrides to a symbol config. Returns merged config."""
    sym = symbol.upper()
    if sym not in _runtime_overrides:
        _runtime_overrides[sym] = {}
    _runtime_overrides[sym].update(updates)
    return get_symbol_config(sym)


def get_all_symbol_configs() -> dict[str, dict[str, Any]]:
    """Return all symbol configs with overrides merged."""
    result = {}
    for sym in SYMBOL_CONFIGS:
        result[sym] = get_symbol_config(sym)
    return result


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
    collector_symbols: str = "XRPUSDT,BTCUSDT,AVAXUSDT,DOGEUSDT,ETHUSDT,SOLUSDT,XAGUSDT,MYXUSDT,ZECUSDT,1000PEPEUSDT,DOTUSDT,NEARUSDT"
    collector_interval: str = "5m"

    # ── Binance Futures Trading ─────────────────────
    binance_api_key: str = ""
    binance_api_secret: str = ""
    # 2. Binance hesabi (HA Reversal ping-pong icin)
    binance_api_key_b: str = ""
    binance_api_secret_b: str = ""
    binance_testnet: bool = True       # Safety: default to testnet
    trading_enabled: bool = True       # Auto-trade active
    stop_loss_pct: float = 0.015      # 1.5% stop-loss (grid search optimal)
    take_profit_pct: float = 0.005    # 0.5% take-profit (grid search optimal)
    binance_proxy_url: str = ""       # SOCKS5 proxy for static IP
    trading_symbols: str = "XRPUSDT,BTCUSDT,AVAXUSDT,DOGEUSDT,ETHUSDT,SOLUSDT,XAGUSDT,MYXUSDT,ZECUSDT,1000PEPEUSDT,DOTUSDT,NEARUSDT"  # Comma-separated whitelist

    # ── Flip Watcher (HTF ters donus tespiti) ─────────
    # Fill sonrasi Pine PLACE_SL beklenir. Bar close + margin sure icinde SL gelmezse
    # HTF renk donmus demektir — backend mini-TP (flip_exit_pct) + SL (flip_sl_pct) koyar.
    # DEPRECATED: flip_watcher timer-based sistem devre disi. Yerine event-driven
    # HTF_STATUS action + _handle_htf_status kullanilir (Pine HTF Status Monitor v1).
    flip_mode_enabled: bool = False
    flip_exit_pct: float = 0.0018       # %0.18 mini-kar cikisi (HTF_STATUS fallback default)
    flip_sl_pct: float = 0.005          # %0.5 SL (HTF_STATUS fallback default)
    flip_check_margin_ms: int = 5_000   # (flip_watcher devre disi — kullanilmaz)

    # v3.10: Fill aninda gecici SL — Pine PLACE_SL bar close'ta gelene kadar
    # poz korumasiz kalmasin. Pine PLACE_SL geldiginde gecici iptal + Pine SL.
    emergency_sl_enabled: bool = True
    emergency_sl_pct: float = 0.02      # %2 gecici SL

    # v3.11: CANCEL protokolu — Pine 1 CANCEL geldiginde poz varsa
    # muhafazakar TP + SL. Pine "bu sinyal hatali" diyor, kucuk karla cikma
    # hedefi + guvenli SL. Mum kapanisinda POSITION_STATUS override eder.
    cancel_protocol_enabled: bool = True
    cancel_tp_pct: float = 0.002        # %0.20 kar (mini TP)
    cancel_sl_pct: float = 0.02         # %2 zarar (guvenlik SL)

    # v3.17: Pine Chaser — ters pozdan market ile ciktiktan sonra Pine 1'in
    # dogru yon TP hedefine kar mesafesi olusursa market entry yap.
    pine_chaser_enabled: bool = True
    pine_chaser_min_profit_pct: float = 0.003    # %0.30 min kar
    pine_chaser_tick_sec: int = 1                # tick araligi
    pine_chaser_verify_retries: int = 3          # market order verify retry
    pine_chaser_verify_backoff_sec: float = 0.5  # ilk backoff (× 4 exponential)

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
