"""POST /st-webhook – SuperTrend signal receiver (TradingView → Trade).

Tek giris noktasi. Filtre/optimizer bypass — sinyal gelir, direkt islem acilir.

Mevcut TradingView alert formati ile tam uyumlu:
{"secret":"...","indicator":"AdaptiveTrendFlow","symbol":"{{ticker}}","tf":"{{interval}}","signal":"BUY","price":"{{close}}","ts":"{{timenow}}"}
"""

from __future__ import annotations


import time
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import settings, get_symbol_config
from app.modules.normalizer import normalize_symbol, normalize_tf
from app.modules.st_signal_logger import log_st_signal
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


_TZ_IST = timezone(timedelta(hours=3))  # Istanbul UTC+3

def _parse_time(raw: str | int | None) -> tuple[int, str]:
    """Parse signal time. Returns (unix_ts, formatted_str in Istanbul time)."""
    now = int(time.time())
    if raw is None:
        dt = datetime.fromtimestamp(now, tz=_TZ_IST)
        return now, dt.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(raw, int):
        dt = datetime.fromtimestamp(raw, tz=_TZ_IST)
        return raw, dt.strftime("%Y-%m-%d %H:%M:%S")

    raw_str = str(raw).strip()

    # Try plain integer
    try:
        ts = int(raw_str)
        dt = datetime.fromtimestamp(ts, tz=_TZ_IST)
        return ts, dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        pass

    # Try ISO datetime formats
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw_str, fmt).replace(tzinfo=timezone.utc)
            dt_ist = dt.astimezone(_TZ_IST)
            return int(dt.timestamp()), dt_ist.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    # Fallback: now
    dt = datetime.fromtimestamp(now, tz=_TZ_IST)
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

    # ── 4. Config kontrolleri — indicator_settings'ten ──
    from app.modules.indicator_settings_store import get_settings_or_defaults
    sym_cfg = await get_settings_or_defaults(symbol)

    # Listening kapali → sinyal loglanir ama isleme alinmaz
    if not sym_cfg.get("listening", True):
        log.info("st_webhook_listening_off", symbol=symbol, direction=direction)
        return JSONResponse(content={
            "status": "listening_off",
            "symbol": symbol,
            "direction": direction,
            "message": f"Signal listening disabled for {symbol}",
        })

    # ── 5. Haftasonu kontrolu (TradFi semboller icin) ──
    from datetime import datetime, timezone, timedelta
    _tz_istanbul = timezone(timedelta(hours=3))
    now_ist = datetime.now(_tz_istanbul)
    weekday = now_ist.weekday()  # 0=Pazartesi, 4=Cuma, 5=Cumartesi, 6=Pazar
    hour = now_ist.hour

    # weekend_closed: Cuma 20:00 → Pazar 23:59 (Istanbul) arasi islem kapatir
    is_weekend_closed = sym_cfg.get("weekend_closed", False) and (
        (weekday == 4 and hour >= 20) or   # Cuma 20:00+
        weekday == 5 or                     # Cumartesi
        weekday == 6                        # Pazar
    )

    if is_weekend_closed:
        log.info("st_webhook_weekend_closed", symbol=symbol, direction=direction, day=weekday, hour=hour)
        return JSONResponse(content={
            "status": "weekend_closed",
            "symbol": symbol,
            "direction": direction,
            "message": f"{symbol} haftasonu kapali (Cuma 20:00 - Pazar 24:00 TR)",
        })

    # ── 6. Log signal ──────────────────────────────────
    row_id = await log_st_signal(
        dt=dt_str,
        symbol=symbol,
        direction=direction,
        band="N/A",
        price=price,
        entered=True,
    )

    # ── 7. Webhook artik islem ACMIYOR — sadece loglama ──
    # Islem acma tamamen signal_engine.py tarafindan yapiliyor.
    # Webhook sinyalleri karsilastirma icin logda kaliyor.
    trade_dispatched = False
    log.info(
        "st_webhook_log_only",
        symbol=symbol,
        direction=direction,
        price=price,
        tf=tf,
        indicator=indicator,
        message="Webhook signal logged, trade execution handled by signal_engine",
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
