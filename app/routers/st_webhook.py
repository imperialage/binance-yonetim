"""POST /st-webhook – SuperTrend signal receiver (TradingView → Trade).

Tek giris noktasi. Filtre/optimizer bypass — sinyal gelir, direkt islem acilir.

Mevcut TradingView alert formati ile tam uyumlu:
{"secret":"...","indicator":"AdaptiveTrendFlow","symbol":"{{ticker}}","tf":"{{interval}}","signal":"BUY","price":"{{close}}","ts":"{{timenow}}"}
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import settings, get_symbol_config
from app.modules.normalizer import normalize_symbol, normalize_tf
from app.modules.st_signal_logger import log_st_signal
from app.modules.trade_executor import execute_trade
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()

# Dedupe: son gorulen sinyal key → unix timestamp
_seen_signals: dict[str, float] = {}
_DEDUPE_WINDOW = 300  # 5 dakika icinde ayni sinyal tekrar gelirse reddet


class STWebhookPayload(BaseModel):
    """TradingView AlgoAlpha SuperTrend webhook payload.

    Hem eski format (signal/ts) hem yeni format (direction/time) kabul eder.
    """

    secret: str
    symbol: str = Field(examples=["ETHUSDT", "BINANCE:ETHUSDT"])
    price: float | str

    # Eski format (mevcut alert'ler)
    signal: str | None = Field(default=None, description="BUY or SELL (eski format)")
    ts: str | int | None = Field(default=None, description="Signal time (eski format)")
    indicator: str | None = Field(default=None, description="Indicator name (loglanir)")

    # Yeni format (opsiyonel, ileride kullanilabilir)
    direction: str | None = Field(default=None, description="BUY or SELL (yeni format)")
    time: str | int | None = Field(default=None, description="Signal time (yeni format)")

    # Ortak
    tf: str | None = Field(default=None, description="Timeframe: 5, 5m, 15m, 1h, etc.")

    model_config = {"extra": "allow"}


def _parse_price(raw: float | str) -> float | None:
    if isinstance(raw, (int, float)):
        return float(raw)
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None


def _parse_time(raw: str | int | None) -> tuple[int, str]:
    """Parse signal time. Returns (unix_ts, formatted_str)."""
    now = int(time.time())
    if raw is None:
        dt = datetime.fromtimestamp(now, tz=timezone.utc)
        return now, dt.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(raw, int):
        dt = datetime.fromtimestamp(raw, tz=timezone.utc)
        return raw, dt.strftime("%Y-%m-%d %H:%M:%S")

    raw_str = str(raw).strip()

    # Try plain integer
    try:
        ts = int(raw_str)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return ts, dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        pass

    # Try ISO datetime formats
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw_str, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp()), dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    # Fallback: now
    dt = datetime.fromtimestamp(now, tz=timezone.utc)
    return now, dt.strftime("%Y-%m-%d %H:%M:%S")


def _is_duplicate(symbol: str, direction: str, tf: str) -> bool:
    """Check if same signal was received within dedupe window."""
    key = f"{symbol}:{direction}:{tf}"
    now = time.time()

    # Cleanup old entries
    expired = [k for k, t in _seen_signals.items() if now - t > _DEDUPE_WINDOW]
    for k in expired:
        del _seen_signals[k]

    if key in _seen_signals:
        return True

    _seen_signals[key] = now
    return False


@router.post("/st-webhook")
async def st_webhook(request: Request) -> JSONResponse:
    # TradingView Content-Type: text/plain olabilir, raw parse et
    import json
    raw_body = await request.body()
    try:
        body = json.loads(raw_body)
    except (json.JSONDecodeError, ValueError):
        return JSONResponse(status_code=400, content={"detail": "Invalid JSON body"})

    payload = STWebhookPayload.model_validate(body)

    # ── 1. Secret dogrulama ────────────────────────────
    if payload.secret != settings.tv_webhook_secret:
        log.warning("st_webhook_invalid_secret")
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid secret"},
        )

    # ── 2. Normalize ───────────────────────────────────
    symbol = normalize_symbol(payload.symbol)

    # direction: "signal" (eski) veya "direction" (yeni) alanini oku
    raw_direction = payload.signal or payload.direction or ""
    direction = raw_direction.strip().upper()
    # LONG/SHORT → BUY/SELL donusumu
    if direction == "LONG":
        direction = "BUY"
    elif direction == "SHORT":
        direction = "SELL"

    if direction not in ("BUY", "SELL"):
        return JSONResponse(
            status_code=400,
            content={"detail": f"Invalid signal/direction: {raw_direction}. Must be BUY or SELL."},
        )

    price = _parse_price(payload.price)
    if price is None or price <= 0:
        return JSONResponse(
            status_code=400,
            content={"detail": f"Invalid price: {payload.price}"},
        )

    # TF normalize (default: config'deki ilk TF)
    raw_tf = payload.tf or settings.trading_timeframes.split(",")[0].strip() or "5m"
    tf = normalize_tf(raw_tf)
    if tf is None:
        tf = "5m"

    # time: "ts" (eski) veya "time" (yeni) alanini oku
    raw_time = payload.ts if payload.ts is not None else payload.time
    signal_ts, dt_str = _parse_time(raw_time)

    indicator = payload.indicator or "SuperTrend"

    log.info(
        "st_webhook_received",
        symbol=symbol,
        direction=direction,
        price=price,
        tf=tf,
        indicator=indicator,
        time=dt_str,
    )

    # ── 3. Dedupe ──────────────────────────────────────
    if _is_duplicate(symbol, direction, tf):
        log.info("st_webhook_duplicate", symbol=symbol, direction=direction, tf=tf)
        return JSONResponse(content={
            "status": "duplicate",
            "symbol": symbol,
            "direction": direction,
            "message": "Same signal received within 5 minutes",
        })

    # ── 4. Config kontrolleri ─────────────────────────
    sym_cfg = get_symbol_config(symbol)

    # Listening kapali → sinyal loglanir ama isleme alinmaz
    if not sym_cfg.get("listening", True):
        log.info("st_webhook_listening_off", symbol=symbol, direction=direction)
        return JSONResponse(content={
            "status": "listening_off",
            "symbol": symbol,
            "direction": direction,
            "message": f"Signal listening disabled for {symbol}",
        })

    # ── 5. Log signal ──────────────────────────────────
    row_id = await log_st_signal(
        dt=dt_str,
        symbol=symbol,
        direction=direction,
        band="N/A",
        price=price,
        entered=True,
    )

    # ── 6. Execute trade ───────────────────────────────
    trade_dispatched = False
    skip_reason = None

    if not settings.trading_enabled:
        skip_reason = "trading_disabled"
    elif not sym_cfg.get("enabled", True):
        skip_reason = "symbol_disabled"

    if skip_reason:
        log.info("st_trade_skipped", symbol=symbol, direction=direction, reason=skip_reason)
    else:
        event_id = f"st-{row_id}-{signal_ts}"

        asyncio.create_task(execute_trade(
            symbol=symbol,
            signal=direction,
            price=price,
            event_id=event_id,
            tf=tf,
            tp_pct=sym_cfg.get("tp_pct"),
            sl_pct=sym_cfg.get("sl_pct"),
        ))
        trade_dispatched = True
        log.info(
            "st_trade_dispatched",
            symbol=symbol,
            direction=direction,
            price=price,
            tf=tf,
            indicator=indicator,
            tp_pct=sym_cfg.get("tp_pct"),
            sl_pct=sym_cfg.get("sl_pct"),
        )

    return JSONResponse(content={
        "status": "accepted",
        "signal_id": row_id,
        "symbol": symbol,
        "direction": direction,
        "price": price,
        "tf": tf,
        "indicator": indicator,
        "trade_dispatched": trade_dispatched,
        "trading_enabled": settings.trading_enabled,
        "skip_reason": skip_reason,
    })
