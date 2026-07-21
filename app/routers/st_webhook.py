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

    Uc format destekler:
      1) Eski (signal/ts): BUY/SELL/CLOSE market — mevcut motorun kabul ettigi
      2) Yeni (direction/time): aynı, alternatif alan isimleri
      3) v3 (action=...): PLACE_LIMIT / CANCEL / PLACE_SL — LIMIT bazli akis

    price alani sadece market ve LIMIT icin zorunlu. PLACE_SL / CANCEL i̇çin
    opsiyonel — handler tarafinda action bazli validate edilir.
    """

    secret: str
    symbol: str = Field(examples=["ETHUSDT", "BINANCE:ETHUSDT"])
    price: float | str | None = Field(default=None, description="LIMIT / MARKET emir fiyati. PLACE_SL i̇çin gerek yok.")

    # v3 action tabanli akis (opsiyonel — yoksa signal/direction backward compat)
    action: str | None = Field(default=None, description="PLACE_LIMIT / CANCEL / PLACE_SL (v3). Yoksa signal/direction eski akis.")
    side: str | None = Field(default=None, description="PLACE_LIMIT: BUY/SELL. PLACE_SL: kapatma yonu (LONG poz i̇çin SELL, SHORT poz i̇çin BUY).")
    stop_price: float | str | None = Field(default=None, description="PLACE_SL i̇çin SL tetik fiyati (mutlak).")
    bar_id: str | None = Field(default=None, description="Pine barin acilis time (ms) — dedupe i̇çin.")
    reason: str | None = Field(default=None, description="CANCEL nedeni (log/audit) — ornek: htf_flip, manual.")

    # Eski format (mevcut alert'ler)
    signal: str | None = Field(default=None, description="BUY or SELL (eski format)")
    ts: str | int | None = Field(default=None, description="Signal time (eski format)")
    indicator: str | None = Field(default=None, description="Indicator name (loglanir)")

    # Yeni format (opsiyonel, ileride kullanilabilir)
    direction: str | None = Field(default=None, description="BUY or SELL (yeni format)")
    time: str | int | None = Field(default=None, description="Signal time (yeni format)")

    # Webhook TP/SL (Pine Script'ten gelen mutlak fiyat degerleri)
    tp: float | str | None = Field(default=None, description="Take profit price from Pine Script")
    sl: float | str | None = Field(default=None, description="Stop loss price from Pine Script (eski BUY/SELL akisi i̇çin)")

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

    # ── v3 action dispatch (PLACE_LIMIT / CANCEL / PLACE_SL) ───────────
    # Pine v3 indikatoru action= alaniyla LIMIT bazli akis kullanir.
    # Eski BUY/SELL/CLOSE market akisi bozulmadan altta devam eder.
    action_v3 = (payload.action or "").strip().upper()
    if action_v3 in ("PLACE_LIMIT", "CANCEL", "PLACE_SL"):
        indicator_v3 = payload.indicator or "webhook_v3"
        if action_v3 == "PLACE_LIMIT":
            return await _handle_place_limit(payload, symbol, indicator_v3)
        if action_v3 == "CANCEL":
            return await _handle_cancel(payload, symbol, indicator_v3)
        if action_v3 == "PLACE_SL":
            return await _handle_place_sl(payload, symbol, indicator_v3)

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

    log.info("webhook_trade_check", symbol=symbol, direction=direction,
             webhook_trade=webhook_trade_enabled,
             trading_enabled=settings.trading_enabled,
             will_trade=bool(webhook_trade_enabled and settings.trading_enabled))

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
                        place_stop_market_instant,
                        place_take_profit_market_order,
                        round_price,
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

                        # SL yerlestir — webhook'tan gelen mutlak fiyat oncelikli
                        # (indikatorun label'da gosterdigi ideal seviye).
                        # Yoksa config sl_pct fallback (fill_price'a gore hesap).
                        sl_side = "SELL" if direction == "BUY" else "BUY"
                        if webhook_sl is not None and webhook_sl > 0:
                            sl_price = round_price(webhook_sl, tick_size)
                            sl_source = "webhook"
                        else:
                            sl_pct = sym_cfg.get("sl_pct", 3.0) / 100.0
                            raw_sl = fill_price * (1 - sl_pct) if direction == "BUY" else fill_price * (1 + sl_pct)
                            sl_price = round_price(raw_sl, tick_size)
                            sl_source = "config"
                        try:
                            await place_stop_market_instant(symbol, sl_side, quantity, sl_price)
                            log.info("webhook_sl_placed", symbol=symbol, sl_price=sl_price, source=sl_source)
                        except Exception as sl_err:
                            log.warning("webhook_sl_failed", symbol=symbol, error=str(sl_err))

                        # TP yerlestir — SADECE webhook'tan geldiyse (config'te
                        # webhook TP yok, indikator alert'i ile gelmezse TP koyulmaz).
                        if webhook_tp is not None and webhook_tp > 0:
                            tp_side = "SELL" if direction == "BUY" else "BUY"
                            tp_price = round_price(webhook_tp, tick_size)
                            try:
                                await place_take_profit_market_order(symbol, tp_side, quantity, tp_price)
                                log.info("webhook_tp_placed", symbol=symbol, tp_price=tp_price, source="webhook")
                            except Exception as tp_err:
                                log.warning("webhook_tp_failed", symbol=symbol, error=str(tp_err))
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


# ════════════════════════════════════════════════════════════════════
# v3 ACTION HANDLERS — PLACE_LIMIT / CANCEL / PLACE_SL
# ════════════════════════════════════════════════════════════════════
# Pine v3 indikatoru LIMIT bazli akis icin action= alanini kullanir.
# Backend burada 3 action'u handler ile isler; eski BUY/SELL/CLOSE
# market akisi yukaridaki ana handler'da devam eder (backward compat).
#
# Akis:
#   PLACE_LIMIT (bar basinda) -> eski pending emri iptal + yeni LIMIT emir + TP fill-sonrasi konur
#   CANCEL (HTF flip)         -> mevcut pending LIMIT emri iptal, Redis temizle
#   PLACE_SL (fill+bar close) -> pozisyon dogrula + STOP_MARKET algo emri
#
# State: Redis (app.modules.webhook_order_tracker)
# Fill tespiti: order_stream.py user-data WS -> webhook_order_tracker.handle_fill_event
# ════════════════════════════════════════════════════════════════════


async def _handle_place_limit(payload: STWebhookPayload, symbol: str, indicator: str) -> JSONResponse:
    """PLACE_LIMIT: bar basinda gelen limit emir talebi.

    Akis:
      1. Sembol config kontrolu (listening, weekend_closed, webhook_trade)
      2. Bar_id dedupe (Redis'te ayni bar_id varsa skip)
      3. Mevcut pending order varsa iptal et
      4. Pozisyon zaten varsa skip (poz kapanana kadar yeni LIMIT koyulmaz)
      5. Balance * weight * 0.98 / price -> qty (step_size + min_notional)
      6. place_limit_order + Redis'e pending kaydet (fill event'i bekle)
    """
    from app.modules import webhook_order_tracker as tracker
    from app.modules.binance_client import (
        cancel_order, get_position_risk, get_total_wallet_balance, place_limit_order,
        round_price, round_step_size,
    )
    from app.modules.indicator_settings_store import get_settings_or_defaults
    from app.modules.trade_executor import get_exchange_info_cached

    side = (payload.side or "").strip().upper()
    if side not in ("BUY", "SELL"):
        return JSONResponse(status_code=400, content={"detail": f"PLACE_LIMIT side gecersiz: {side}"})

    price = _parse_price(payload.price)
    if price is None or price <= 0:
        return JSONResponse(status_code=400, content={"detail": f"PLACE_LIMIT price gecersiz: {payload.price}"})

    webhook_tp = _parse_price(payload.tp) if payload.tp else None
    bar_id = payload.bar_id or str(int(time.time() * 1000))

    # Config kontrolleri
    sym_cfg = await get_settings_or_defaults(symbol)
    if not sym_cfg.get("listening", True):
        log.info("place_limit_listening_off", symbol=symbol)
        return JSONResponse(content={"status": "listening_off", "symbol": symbol})
    if not sym_cfg.get("webhook_trade", False):
        log.info("place_limit_webhook_trade_off", symbol=symbol)
        return JSONResponse(content={"status": "webhook_trade_off", "symbol": symbol})
    if not settings.trading_enabled:
        log.info("place_limit_trading_disabled", symbol=symbol)
        return JSONResponse(content={"status": "trading_disabled", "symbol": symbol})

    # Weekend kontrol
    now_ist = datetime.now(_TZ_IST)
    if sym_cfg.get("weekend_closed", False) and (
        (now_ist.weekday() == 4 and now_ist.hour >= 20)
        or now_ist.weekday() == 5
        or now_ist.weekday() == 6
    ):
        return JSONResponse(content={"status": "weekend_closed", "symbol": symbol})

    # Bar_id dedupe: ayni bar için 2. PLACE_LIMIT gelmisse skip
    existing = await tracker.get_pending_limit(symbol)
    if existing is not None and str(existing.get("bar_id")) == str(bar_id) and existing.get("side") == side:
        # Ayni bar + ayni yon + ayni fiyat -> gerekmiyor, skip
        if abs(float(existing.get("price", 0)) - price) < 1e-8:
            log.info("place_limit_duplicate_bar", symbol=symbol, bar_id=bar_id)
            return JSONResponse(content={"status": "duplicate_bar", "symbol": symbol, "bar_id": bar_id})

    # Pozisyon zaten varsa yeni LIMIT koyma (fill sonrasi tekrar tekrar geliyorsa Pine state=1 -> skip beklenir)
    positions = await get_position_risk(symbol)
    pos_amt = 0.0
    for p in positions:
        if p.get("symbol") == symbol:
            pos_amt = float(p.get("positionAmt", 0))
            break
    if pos_amt != 0:
        log.info("place_limit_position_exists_skip", symbol=symbol, pos_amt=pos_amt)
        return JSONResponse(content={"status": "position_exists", "symbol": symbol, "pos_amt": pos_amt})

    # Mevcut pending order varsa iptal
    if existing is not None:
        old_order_id = existing.get("orderId")
        if old_order_id:
            try:
                await cancel_order(symbol, int(old_order_id))
                log.info("place_limit_cancelled_previous", symbol=symbol, order_id=old_order_id)
            except Exception as e:
                # -2011 (unknown order) benign — muhtemelen zaten fill/cancel oldu
                msg = str(e).lower()
                if "-2011" in msg or "unknown order" in msg or "does not exist" in msg:
                    log.info("place_limit_previous_already_gone", symbol=symbol, order_id=old_order_id)
                else:
                    log.warning("place_limit_cancel_previous_failed", symbol=symbol, order_id=old_order_id, error=str(e))
        await tracker.clear_pending_limit(symbol)

    # Qty hesabi
    balance = await get_total_wallet_balance()
    info = await get_exchange_info_cached(symbol)
    step_size = info["lotSize"]["stepSize"]
    min_qty = float(info["lotSize"]["minQty"])
    tick_size = info["priceFilter"]["tickSize"]
    min_notional = float(info.get("minNotional", {}).get("notional", 5))
    sym_weight = sym_cfg.get("weight", 0.10)

    usable = balance * sym_weight * 0.98
    raw_qty = usable / price if price > 0 else 0.0
    quantity = round_step_size(raw_qty, step_size)

    if quantity < min_qty:
        log.warning("place_limit_qty_too_low", symbol=symbol, qty=quantity, min=min_qty)
        return JSONResponse(status_code=400, content={"status": "qty_too_low", "qty": quantity, "min_qty": min_qty})
    if float(quantity) * price < min_notional:
        log.warning("place_limit_notional_too_low", symbol=symbol,
                    notional=float(quantity) * price, min=min_notional)
        return JSONResponse(status_code=400, content={
            "status": "notional_too_low",
            "notional": float(quantity) * price,
            "min_notional": min_notional,
        })

    # Fiyat quantize (tick_size)
    limit_price = round_price(price, tick_size)

    # clientOrderId prefix "wh-" — fill event handler bu prefix'e bakarak webhook order oldugunu anlar
    client_order_id = f"wh-{int(time.time() * 1000)}"[:36]

    try:
        result = await place_limit_order(symbol, side, float(quantity), float(limit_price))
        order_id = result.get("orderId")
        log.info("place_limit_placed", symbol=symbol, side=side, price=limit_price,
                 qty=quantity, order_id=order_id, tp=webhook_tp)
    except Exception as e:
        log.error("place_limit_failed", symbol=symbol, side=side, error=str(e))
        return JSONResponse(status_code=500, content={"status": "error", "detail": str(e)})

    # Redis'e pending kaydet (fill event bekleyecek)
    await tracker.set_pending_limit(
        symbol,
        order_id=order_id or 0,
        client_order_id=client_order_id,
        side=side,
        price=float(limit_price),
        tp=webhook_tp,
        qty=float(quantity),
        bar_id=bar_id,
        tf=str(payload.tf or ""),
    )

    return JSONResponse(content={
        "status": "placed",
        "action": "PLACE_LIMIT",
        "symbol": symbol,
        "side": side,
        "price": float(limit_price),
        "qty": float(quantity),
        "tp": webhook_tp,
        "order_id": order_id,
        "bar_id": bar_id,
    })


async def _handle_cancel(payload: STWebhookPayload, symbol: str, indicator: str) -> JSONResponse:
    """CANCEL: HTF flip'te bekleyen LIMIT emri iptal.

    Redis'ten pending order al, Binance'ta iptal, Redis'i temizle.
    """
    from app.modules import webhook_order_tracker as tracker
    from app.modules.binance_client import cancel_order

    reason = (payload.reason or "unknown").strip()

    existing = await tracker.get_pending_limit(symbol)
    if existing is None:
        log.info("cancel_no_pending", symbol=symbol)
        return JSONResponse(content={"status": "no_pending", "symbol": symbol})

    order_id = existing.get("orderId")
    if not order_id:
        await tracker.clear_pending_limit(symbol)
        return JSONResponse(content={"status": "cleared_no_orderid", "symbol": symbol})

    try:
        await cancel_order(symbol, int(order_id))
        log.info("cancel_ok", symbol=symbol, order_id=order_id, reason=reason)
        result_status = "cancelled"
    except Exception as e:
        msg = str(e).lower()
        if "-2011" in msg or "unknown order" in msg or "does not exist" in msg:
            # Binance'ta yok — muhtemelen fill oldu ya da zaten iptal
            log.info("cancel_already_gone", symbol=symbol, order_id=order_id, reason=reason)
            result_status = "already_gone"
        else:
            log.warning("cancel_failed", symbol=symbol, order_id=order_id, error=str(e))
            # Redis'i temizleme — sonraki PLACE_LIMIT tekrar deneyecek
            return JSONResponse(status_code=500, content={"status": "error", "detail": str(e)})

    await tracker.clear_pending_limit(symbol)
    return JSONResponse(content={
        "status": result_status,
        "action": "CANCEL",
        "symbol": symbol,
        "order_id": order_id,
        "reason": reason,
    })


async def _handle_place_sl(payload: STWebhookPayload, symbol: str, indicator: str) -> JSONResponse:
    """PLACE_SL: fill olan mumun kapanisinda backend'e SL koy.

    Pine v3 fill'den sonra ilk bar kapanisinda bu alert'i atar.
    Idempotency: Redis flag ile ayni poz i̇çin 2. PLACE_SL skip.
    Poz varligini dogrular, sonra STOP_MARKET algo emri koyar.
    """
    from app.modules import webhook_order_tracker as tracker
    from app.modules.binance_client import (
        get_position_risk, place_stop_market_instant, round_price,
    )
    from app.modules.trade_executor import get_exchange_info_cached

    side = (payload.side or "").strip().upper()
    if side not in ("BUY", "SELL"):
        return JSONResponse(status_code=400, content={"detail": f"PLACE_SL side gecersiz: {side}"})

    stop_price = _parse_price(payload.stop_price)
    if stop_price is None or stop_price <= 0:
        return JSONResponse(status_code=400, content={"detail": f"PLACE_SL stop_price gecersiz: {payload.stop_price}"})

    # Idempotency — ayni poz için ikinci PLACE_SL
    if await tracker.is_sl_placed(symbol):
        log.info("place_sl_already_placed", symbol=symbol)
        return JSONResponse(content={"status": "already_placed", "symbol": symbol})

    # Pozisyon var mi
    positions = await get_position_risk(symbol)
    pos_amt = 0.0
    for p in positions:
        if p.get("symbol") == symbol:
            pos_amt = float(p.get("positionAmt", 0))
            break
    if pos_amt == 0:
        log.warning("place_sl_no_position", symbol=symbol)
        return JSONResponse(content={"status": "no_position", "symbol": symbol})

    # Yon sanity check: LONG poz (positionAmt>0) i̇çin SL kapatma yonu SELL,
    # SHORT poz (<0) i̇çin BUY olmali.
    expected_side = "SELL" if pos_amt > 0 else "BUY"
    if side != expected_side:
        log.warning("place_sl_side_mismatch", symbol=symbol, pos_amt=pos_amt,
                    got=side, expected=expected_side)
        # Yine de expected_side ile devam et — Pine tarafi hatasi olabilir, poz gercektir
        side = expected_side

    # Tick round
    info = await get_exchange_info_cached(symbol)
    tick_size = info["priceFilter"]["tickSize"]
    stop_px = round_price(stop_price, tick_size)
    qty = abs(pos_amt)

    try:
        await place_stop_market_instant(symbol, side, qty, float(stop_px))
        await tracker.mark_sl_placed(symbol)
        log.info("place_sl_ok", symbol=symbol, side=side, stop_price=stop_px, qty=qty)
        return JSONResponse(content={
            "status": "placed",
            "action": "PLACE_SL",
            "symbol": symbol,
            "side": side,
            "stop_price": float(stop_px),
            "qty": qty,
        })
    except Exception as e:
        log.error("place_sl_failed", symbol=symbol, error=str(e))
        return JSONResponse(status_code=500, content={"status": "error", "detail": str(e)})


# ════════════════════════════════════════════════════════════════════
# FILL EVENT HANDLER — order_stream.py cagirir
# ════════════════════════════════════════════════════════════════════

async def handle_fill_event(order: dict) -> None:
    """LIMIT fill oldugunda backend TP algo emrini koyar.

    order_stream.py ORDER_TRADE_UPDATE event'inde clientOrderId "wh-"
    prefix'i ile filtreleyerek burayi cagirir.

    Fill isleme:
      1. clientOrderId "wh-" prefix mi? Degilse skip.
      2. Redis'ten pending order al — orderId match mi?
      3. TP fiyati varsa TAKE_PROFIT_MARKET algo emri koy.
      4. Pending order'i sil (fill tamamlandi).
      5. SL flag = FALSE (bar close'ta Pine PLACE_SL alert'i gelecek).

    Pozisyon kapanisinda (reduceOnly veya positionAmt=0) tum state temizlenir.
    """
    from app.modules import webhook_order_tracker as tracker
    from app.modules.binance_client import place_take_profit_market_order

    client_order_id = str(order.get("c", ""))
    order_id = str(order.get("i", ""))
    symbol = order.get("s", "")
    status = order.get("X", "")
    order_type = order.get("ot", "")
    reduce_only = bool(order.get("R", False))

    if not symbol:
        return

    # Kapanış eventi (SL veya TP algo tetigi) — Redis state'i temizle
    if reduce_only and status == "FILLED":
        await tracker.clear_all_state(symbol)
        log.info("webhook_fill_close_state_cleared", symbol=symbol, order_type=order_type)
        return

    # Sadece FILLED LIMIT emirlerini isle
    if status != "FILLED":
        return
    if order_type not in ("LIMIT", ""):
        return

    # Match via orderId: bizim koydugumuz emir Redis'te. clientOrderId prefix
    # yerine orderId lookup — place_limit_order client_order_id kabul etmiyor,
    # Binance kendi ID'sini uretir.
    pending = await tracker.get_pending_limit(symbol)
    if pending is None:
        # Baska bir emir (motor, HA engine vb.) — bize ait degil
        return
    pending_order_id = str(pending.get("orderId", ""))
    if pending_order_id != order_id:
        # Redis'teki pending baska bir order — bu fill bize ait degil
        log.debug("webhook_fill_orderid_mismatch", symbol=symbol,
                  fill_order_id=order_id, pending_order_id=pending_order_id)
        return

    # Fill bilgileri
    filled_qty = float(order.get("z", 0) or order.get("q", 0))  # 'z' = cumulative filled qty
    avg_price = float(order.get("ap", 0))
    tp_price = pending.get("tp")
    side = str(pending.get("side", "")).upper()
    close_side = "SELL" if side == "BUY" else "BUY"

    log.info("webhook_fill_detected", symbol=symbol, side=side, avg_price=avg_price,
             qty=filled_qty, client_order_id=client_order_id)

    # TP algo emri (varsa)
    if tp_price is not None and tp_price > 0:
        try:
            await place_take_profit_market_order(symbol, close_side, filled_qty, float(tp_price))
            log.info("webhook_tp_placed_on_fill", symbol=symbol, tp_price=tp_price, source="webhook_v3")
        except Exception as e:
            log.error("webhook_tp_place_failed_on_fill", symbol=symbol, error=str(e))
    else:
        log.info("webhook_no_tp_in_pending", symbol=symbol)

    # Pending'i temizle (fill tamamlandi). SL flag DOKUNMA — bar close'ta Pine PLACE_SL gelecek.
    await tracker.clear_pending_limit(symbol)
