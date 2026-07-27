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
    action: str | None = Field(default=None, description="PLACE_LIMIT / CANCEL / PLACE_SL / HTF_STATUS (v3.6).")
    side: str | None = Field(default=None, description="PLACE_LIMIT: BUY/SELL. PLACE_SL: kapatma yonu (LONG poz i̇çin SELL, SHORT poz i̇çin BUY).")
    stop_price: float | str | None = Field(default=None, description="PLACE_SL i̇çin SL tetik fiyati (mutlak).")
    bar_id: str | None = Field(default=None, description="Pine barin acilis time (ms) — dedupe i̇çin.")
    reason: str | None = Field(default=None, description="CANCEL nedeni (log/audit) — ornek: htf_flip, manual.")
    htf_green: bool | None = Field(default=None, description="HTF_STATUS: kapanmis HTF HA mumun rengi (True=yesil, False=kirmizi).")
    sl_pct: float | str | None = Field(default=None, description="Pine SL yuzdesi (0.005 = %0.5). HTF_STATUS flip exit'te kullanilir.")
    flip_pct: float | str | None = Field(default=None, description="Pine flip exit yuzdesi (0.0018 = %0.18).")

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
    if action_v3 in ("PLACE_LIMIT", "CANCEL", "PLACE_SL", "HTF_STATUS"):
        indicator_v3 = payload.indicator or "webhook_v3"
        if action_v3 == "PLACE_LIMIT":
            return await _handle_place_limit(payload, symbol, indicator_v3)
        if action_v3 == "CANCEL":
            return await _handle_cancel(payload, symbol, indicator_v3)
        if action_v3 == "PLACE_SL":
            return await _handle_place_sl(payload, symbol, indicator_v3)
        if action_v3 == "HTF_STATUS":
            return await _handle_htf_status(payload, symbol, indicator_v3)

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

    v3.7 degisiklik: Poz varsa (fill oldu) CANCEL BLOKLANIR. Fill bar close
    kararini HTF_STATUS handler verecek (Pine 1 protokolu ya Pine 2 protokolu).
    """
    from app.modules import webhook_order_tracker as tracker
    from app.modules.binance_client import cancel_order, get_position_risk

    reason = (payload.reason or "unknown").strip()

    # v3.7: Poz varsa CANCEL'i uygulama — fill bar close karari HTF_STATUS'un
    try:
        positions = await get_position_risk(symbol)
        pos_amt = 0.0
        for p in positions:
            if p.get("symbol") == symbol:
                pos_amt = float(p.get("positionAmt", 0))
                break
        if pos_amt != 0:
            log.info("cancel_blocked_position_open", symbol=symbol, pos_amt=pos_amt, reason=reason)
            return JSONResponse(content={
                "status": "blocked_position_open",
                "symbol": symbol,
                "pos_amt": pos_amt,
                "reason": reason,
                "message": "Fill bar close bekleniyor, HTF_STATUS karar verecek",
            })
    except Exception as e:
        log.warning("cancel_position_check_failed", symbol=symbol, error=str(e))
        # position check hata verirse yine de eski davranisa dus (guvenli taraf: iptal)

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
        # v3.7: Pine 1 protokolu tamamlandi — karar verildi olarak isaretle.
        # Sonraki HTF_STATUS'lar (fill bar sonrasi flip) ignore edilecek.
        await tracker.mark_flip_decided(symbol)
        await tracker.clear_fill_bar_check(symbol)
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
# HTF_STATUS HANDLER (v3.6 YENI) — event-driven flip exit
# ════════════════════════════════════════════════════════════════════
# Pine her chart bar barstate.isconfirmed'de bu alert'i atar:
#   {"secret":..., "symbol":..., "action":"HTF_STATUS",
#    "htf_green": true/false, "sl_pct": 0.005, "flip_pct": 0.0018, "bar_id":...}
#
# Backend:
#   1. Aktif poz yoksa: sadece logla, cik
#   2. Poz yonu ile HTF ters mi?
#      LONG (positionAmt>0)  + htf_green=false -> FLIP
#      SHORT (positionAmt<0) + htf_green=true  -> FLIP
#   3. Flip ise: mevcut algo emirleri iptal + mini-TP (flip_pct) + SL (sl_pct)
#      Payload'daki yuzdeler Pine ile senkron (flip_watcher'in "kendi hesabi" sorunu yok).
# Idempotency: Redis key `webhook_htf_flip:{sym}:{bar_id}` TTL=1h -> ayni bar 2. HTF_STATUS skip.


async def _handle_htf_status(payload: STWebhookPayload, symbol: str, indicator: str) -> JSONResponse:
    """HTF_STATUS: Pine'dan gelen HTF renk bildirimi. Poz varsa ters mi kontrol et."""
    from app.modules import webhook_order_tracker as tracker
    from app.modules.binance_client import (
        cancel_all_open_orders,
        get_position_risk,
        place_stop_market_instant,
        place_take_profit_market_order,
        round_price,
    )
    from app.modules.redis_client import get_redis
    from app.modules.trade_executor import get_exchange_info_cached

    if payload.htf_green is None:
        return JSONResponse(status_code=400, content={"detail": "htf_green gerekli"})
    htf_green = bool(payload.htf_green)

    # sl_pct / flip_pct payload'tan; yoksa settings default
    try:
        sl_pct = float(payload.sl_pct) if payload.sl_pct is not None else settings.flip_sl_pct
    except (ValueError, TypeError):
        sl_pct = settings.flip_sl_pct
    try:
        flip_pct = float(payload.flip_pct) if payload.flip_pct is not None else settings.flip_exit_pct
    except (ValueError, TypeError):
        flip_pct = settings.flip_exit_pct

    bar_id_str = payload.bar_id or str(int(time.time() * 1000))
    try:
        bar_id_ms = int(bar_id_str)
    except (ValueError, TypeError):
        bar_id_ms = int(time.time() * 1000)

    # 1) Aktif poz var mi?
    positions = await get_position_risk(symbol)
    pos_amt = 0.0
    for p in positions:
        if p.get("symbol") == symbol:
            pos_amt = float(p.get("positionAmt", 0))
            break

    if pos_amt == 0:
        return JSONResponse(content={
            "status": "no_position", "symbol": symbol, "htf_green": htf_green,
        })

    # 2) v3.7: Karar zaten verilmis mi? (sonraki HTF_STATUS'lar ignore)
    if await tracker.is_flip_decided(symbol):
        return JSONResponse(content={
            "status": "decided_earlier", "symbol": symbol,
        })

    # 3) v3.7: Fill bar close geldi mi? Set edilmediyse (fill event kaydetmemis)
    #    veya bar_id fill_bar_id'den kucukse — henuz fill bar kapanmadi, bekle.
    fill_bar_id = await tracker.get_fill_bar_check(symbol)
    if fill_bar_id is None:
        return JSONResponse(content={
            "status": "no_fill_check_pending", "symbol": symbol,
            "message": "Fill event henuz gorulmedi ya da karar zaten verilmis",
        })
    if bar_id_ms < fill_bar_id:
        return JSONResponse(content={
            "status": "waiting_fill_bar_close", "symbol": symbol,
            "htf_bar_id": bar_id_ms, "fill_bar_id": fill_bar_id,
        })

    # 4) Ters mi?
    #   LONG (pos_amt > 0) + htf_green=False -> ters
    #   SHORT (pos_amt < 0) + htf_green=True -> ters
    is_flip = (pos_amt > 0 and not htf_green) or (pos_amt < 0 and htf_green)

    if not is_flip:
        # Pine 1 protokolu — PLACE_SL kendi isini yapsin. Karar verildi, sonraki
        # HTF_STATUS'lar ignore edilsin.
        await tracker.mark_flip_decided(symbol)
        await tracker.clear_fill_bar_check(symbol)
        log.info("htf_status_aligned", symbol=symbol, htf_green=htf_green, pos_amt=pos_amt)
        return JSONResponse(content={
            "status": "aligned", "symbol": symbol, "htf_green": htf_green, "pos_amt": pos_amt,
        })

    # Meta poz bilgisi — entry Binance'tan
    entry = 0.0
    for p in positions:
        if p.get("symbol") == symbol:
            entry = float(p.get("entryPrice", 0))
            break
    if entry <= 0:
        log.warning("htf_status_no_entry_price", symbol=symbol)
        return JSONResponse(content={"status": "no_entry_price", "symbol": symbol})

    qty = abs(pos_amt)
    is_long = pos_amt > 0
    close_side = "SELL" if is_long else "BUY"

    # 4) Mevcut algo emirleri iptal (eski TP dahil, eski SL varsa da)
    try:
        await cancel_all_open_orders(symbol)
    except Exception as e:
        log.warning("htf_flip_cancel_failed", symbol=symbol, error=str(e))

    # 5) Fiyatlari hesapla + tick round
    info = await get_exchange_info_cached(symbol)
    tick_size = info["priceFilter"]["tickSize"]

    if is_long:
        mini_tp_raw = entry * (1 + flip_pct)
        sl_raw = entry * (1 - sl_pct)
    else:
        mini_tp_raw = entry * (1 - flip_pct)
        sl_raw = entry * (1 + sl_pct)

    mini_tp = round_price(mini_tp_raw, tick_size)
    sl_px = round_price(sl_raw, tick_size)

    # 6) Mini-TP
    tp_ok = False
    tp_err = None
    try:
        await place_take_profit_market_order(symbol, close_side, qty, float(mini_tp))
        tp_ok = True
    except Exception as e:
        tp_err = str(e)
        log.error("htf_flip_mini_tp_failed", symbol=symbol, error=tp_err,
                  mini_tp=float(mini_tp), entry=entry)

    # 7) SL
    sl_ok = False
    sl_err = None
    try:
        await place_stop_market_instant(symbol, close_side, qty, float(sl_px))
        await tracker.mark_sl_placed(symbol)  # gec gelen Pine PLACE_SL alert'i skip olsun
        sl_ok = True
    except Exception as e:
        sl_err = str(e)
        log.error("htf_flip_sl_failed", symbol=symbol, error=sl_err,
                  sl=float(sl_px), entry=entry)

    # Karar verildi — sonraki HTF_STATUS'lar ignore, fill_bar_check temizle
    await tracker.mark_flip_decided(symbol)
    await tracker.clear_fill_bar_check(symbol)

    log.warning("htf_flip_activated", symbol=symbol, entry=entry, is_long=is_long, qty=qty,
                mini_tp=float(mini_tp), sl=float(sl_px), tp_ok=tp_ok, sl_ok=sl_ok,
                sl_pct=sl_pct, flip_pct=flip_pct)

    return JSONResponse(content={
        "status": "flip_activated",
        "symbol": symbol,
        "entry": entry,
        "is_long": is_long,
        "qty": qty,
        "mini_tp": float(mini_tp),
        "sl": float(sl_px),
        "tp_ok": tp_ok,
        "sl_ok": sl_ok,
        "tp_err": tp_err,
        "sl_err": sl_err,
    })


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

    # Kapanış eventi (SL veya TP algo tetigi) — orphan algo temizle + Redis
    if reduce_only and status == "FILLED":
        # KRITIK: SL tetiklenirse Binance'ta kalan TP algo emri (veya tersi)
        # orphan kalir. cancel_all_open_orders hem klasik hem algo iptal eder.
        try:
            from app.modules.binance_client import cancel_all_open_orders
            await cancel_all_open_orders(symbol)
        except Exception as e:
            log.warning("webhook_fill_cancel_orphan_failed", symbol=symbol, error=str(e))
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

    # v3.7: Fill bar close karari HTF_STATUS'a birak — fill_bar_id_ms kaydet
    try:
        fill_bar_id_ms = int(str(pending.get("bar_id") or "0") or "0")
        if fill_bar_id_ms > 0:
            await tracker.set_fill_bar_check(symbol, fill_bar_id_ms)
            log.info("fill_bar_check_armed", symbol=symbol, fill_bar_id_ms=fill_bar_id_ms)
    except Exception as e:
        log.warning("fill_bar_check_set_failed", symbol=symbol, error=str(e))

    # Pending'i temizle (fill tamamlandi). SL flag DOKUNMA — bar close'ta HTF_STATUS
    # ya da Pine 1 PLACE_SL alerti gelecek.
    await tracker.clear_pending_limit(symbol)
