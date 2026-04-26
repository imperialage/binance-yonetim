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

    # Webhook TP/SL (Pine Script'ten gelen mutlak fiyat degerleri)
    tp: float | str | None = Field(default=None, description="Take profit price from Pine Script")
    sl: float | str | None = Field(default=None, description="Stop loss price from Pine Script")

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

    if direction not in ("BUY", "SELL", "CLOSE"):
        return JSONResponse(
            status_code=400,
            content={"detail": f"Invalid signal/direction: {raw_direction}. Must be BUY, SELL or CLOSE."},
        )

    price = _parse_price(payload.price)
    if price is None or price <= 0:
        return JSONResponse(
            status_code=400,
            content={"detail": f"Invalid price: {payload.price}"},
        )

    # Webhook TP/SL (Pine Script'ten gelen mutlak fiyatlar, opsiyonel)
    webhook_tp = _parse_price(payload.tp) if payload.tp else None
    webhook_sl = _parse_price(payload.sl) if payload.sl else None

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

    # ── 6. webhook_trade kontrolu ────────────────────────
    webhook_trade_enabled = sym_cfg.get("webhook_trade", False)
    trade_dispatched = False

    if webhook_trade_enabled and settings.trading_enabled:
        # ── Webhook trade AKTIF — BUY/SELL/CLOSE islem ac/kapat ──
        import asyncio as _asyncio
        from app.modules.binance_client import (
            get_position_risk, place_market_order, cancel_all_open_orders,
            get_total_wallet_balance, round_step_size,
        )
        from app.modules.trade_executor import get_exchange_info_cached

        row_id = await log_st_signal(
            dt=dt_str, symbol=symbol, direction=direction, band=tf,
            price=price, entered=False, source="webhook",
            webhook_tp=webhook_tp, webhook_sl=webhook_sl,
        )

        try:
            if direction == "CLOSE":
                # Mevcut pozisyonu kapat
                positions = await get_position_risk(symbol)
                pos_amt = 0.0
                for p in positions:
                    if p.get("symbol") == symbol:
                        pos_amt = float(p.get("positionAmt", 0))
                        break
                if pos_amt != 0:
                    try:
                        await cancel_all_open_orders(symbol)
                    except Exception:
                        pass
                    close_side = "SELL" if pos_amt > 0 else "BUY"
                    await place_market_order(symbol, close_side, abs(pos_amt), reduce_only=True)
                    trade_dispatched = True
                    log.info("webhook_close_executed", symbol=symbol, qty=abs(pos_amt))
                else:
                    log.info("webhook_close_no_position", symbol=symbol)

            else:
                # BUY veya SELL — pozisyon ac
                # Ters pozisyon varsa once kapat
                positions = await get_position_risk(symbol)
                pos_amt = 0.0
                for p in positions:
                    if p.get("symbol") == symbol:
                        pos_amt = float(p.get("positionAmt", 0))
                        break

                if pos_amt != 0:
                    is_same = (pos_amt > 0 and direction == "BUY") or (pos_amt < 0 and direction == "SELL")
                    if is_same:
                        log.info("webhook_same_side_skip", symbol=symbol, direction=direction)
                    else:
                        # Ters pozisyon kapat
                        try:
                            await cancel_all_open_orders(symbol)
                        except Exception:
                            pass
                        close_side = "SELL" if pos_amt > 0 else "BUY"
                        await place_market_order(symbol, close_side, abs(pos_amt), reduce_only=True)
                        log.info("webhook_reverse_closed", symbol=symbol, side=close_side)
                        pos_amt = 0.0

                if pos_amt == 0:
                    # Yeni pozisyon ac (market order)
                    from app.modules.binance_client import (
                        place_stop_market_instant, round_price,
                    )
                    from app.modules.price_stream import get_live_price

                    balance = await get_total_wallet_balance()
                    info = await get_exchange_info_cached(symbol)
                    step_size = info["lotSize"]["stepSize"]
                    min_qty = info["lotSize"]["minQty"]
                    tick_size = info["priceFilter"]["tickSize"]
                    sym_weight = sym_cfg.get("weight", 0.10)

                    live_price = get_live_price(symbol) or price
                    usable = balance * sym_weight * 0.98
                    raw_qty = usable / live_price
                    quantity = round_step_size(raw_qty, step_size)

                    if quantity >= min_qty:
                        side = "BUY" if direction == "BUY" else "SELL"
                        result = await place_market_order(symbol, side, quantity)
                        fill_price = float(result.get("avgPrice", 0)) or live_price
                        trade_dispatched = True
                        log.info("webhook_trade_opened", symbol=symbol, direction=direction,
                                 fill_price=fill_price, qty=quantity)

                        # %3 Guvenlik SL
                        sl_pct = sym_cfg.get("sl_pct", 3.0) / 100.0
                        if direction == "BUY":
                            sl_price = round_price(fill_price * (1 - sl_pct), tick_size)
                            sl_side = "SELL"
                        else:
                            sl_price = round_price(fill_price * (1 + sl_pct), tick_size)
                            sl_side = "BUY"
                        try:
                            await place_stop_market_instant(symbol, sl_side, quantity, sl_price)
                            log.info("webhook_sl_placed", symbol=symbol, sl_price=sl_price)
                        except Exception as sl_err:
                            log.warning("webhook_sl_failed", symbol=symbol, error=str(sl_err))
                    else:
                        log.warning("webhook_qty_too_low", symbol=symbol, qty=quantity, min=min_qty)

        except Exception as e:
            log.error("webhook_trade_error", symbol=symbol, direction=direction, error=str(e))
            import traceback
            log.error("webhook_trade_traceback", tb=traceback.format_exc())

    else:
        # Webhook trade devre disi — sadece logla
        row_id = await log_st_signal(
            dt=dt_str, symbol=symbol, direction=direction, band=tf,
            price=price, entered=False, source="webhook",
            webhook_tp=webhook_tp, webhook_sl=webhook_sl,
        )
        log.info("st_webhook_logged_only", symbol=symbol, direction=direction, price=price)

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
    })
