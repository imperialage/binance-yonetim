"""Webhook order state tracker — Redis-backed persistence.

TradingView Pine v3 alertlerinin backend state'ini tutar. Sistem 3 kritik
durum tutar (Redis key format):

    webhook_limit:{SYMBOL}   -> pending LIMIT emir bilgisi (JSON)
        {orderId, clientOrderId, side, price, tp, qty, bar_id, placed_at}
        TTL: 6 saat (dolmayan emirler otomatik unut)

    webhook_sl_placed:{SYMBOL} -> "1" (idempotency flag)
        Fill sonrasi PLACE_SL alert isleniyorsa 2. alert skip edilir.
        TTL: 24 saat (poz kapaninca zaten silinir, TTL guvenlik agi)

    webhook_pos_meta:{SYMBOL} -> pozisyon meta (JSON)
        {side, entry_price, qty, tp_price, sl_pending, opened_at}
        Fill sonrasi TP koyup meta yazariz. Poz kapanisinda tp_algo id vb.
        TTL: 24 saat.

Bu modul saf state — Binance API call yapmaz. st_webhook.py ve
order_stream.py tarafindan cagirilir.
"""

from __future__ import annotations

import json
import time
from typing import Any

from app.modules.redis_client import get_redis
from app.utils.logging import get_logger

log = get_logger(__name__)

# ── Redis key builders ──────────────────────────────────────────────────
KEY_LIMIT = "webhook_limit:{sym}"
KEY_SL_PLACED = "webhook_sl_placed:{sym}"
KEY_POS_META = "webhook_pos_meta:{sym}"

# TTL değerleri (saniye)
TTL_LIMIT = 6 * 60 * 60      # 6 saat — dolmayan pending emri unut
TTL_SL_FLAG = 24 * 60 * 60   # 24 saat — poz kapanisinda silinir zaten
TTL_POS_META = 24 * 60 * 60  # 24 saat


def _key(pattern: str, symbol: str) -> str:
    return pattern.format(sym=symbol.upper())


# ── Pending LIMIT order state ───────────────────────────────────────────

async def get_pending_limit(symbol: str) -> dict[str, Any] | None:
    """Bir sembol icin bekleyen LIMIT emir bilgisini getir."""
    try:
        r = await get_redis()
        raw = await r.get(_key(KEY_LIMIT, symbol))
        if raw is None:
            return None
        return json.loads(raw)
    except Exception as e:
        await log.awarning("webhook_tracker_get_limit_failed", symbol=symbol, error=str(e))
        return None


async def set_pending_limit(
    symbol: str,
    *,
    order_id: int | str,
    client_order_id: str,
    side: str,
    price: float,
    tp: float | None,
    qty: float,
    bar_id: str,
    tf: str = "",
) -> None:
    """Yeni bekleyen LIMIT emir bilgisini kaydet (fill event beklenene kadar).

    tf: Pine indikator TF (ornek: "15", "60", "5m", "1h"). Poller bar close
    zamanini hesaplamak icin kullanir — TF'e gore bar close'a yaklastigi anda
    Binance'a order status sorusu atar.
    """
    payload = {
        "orderId": str(order_id),
        "clientOrderId": client_order_id,
        "side": side.upper(),
        "price": float(price),
        "tp": float(tp) if tp is not None else None,
        "qty": float(qty),
        "bar_id": str(bar_id),
        "tf": str(tf),
        "placed_at": int(time.time()),
    }
    try:
        r = await get_redis()
        await r.set(_key(KEY_LIMIT, symbol), json.dumps(payload), ex=TTL_LIMIT)
    except Exception as e:
        await log.awarning("webhook_tracker_set_limit_failed", symbol=symbol, error=str(e))


async def clear_pending_limit(symbol: str) -> None:
    """Bekleyen LIMIT emir kaydini sil (cancel, fill sonrasi vb.)."""
    try:
        r = await get_redis()
        await r.delete(_key(KEY_LIMIT, symbol))
    except Exception as e:
        await log.awarning("webhook_tracker_clear_limit_failed", symbol=symbol, error=str(e))


# ── SL placed idempotency flag ──────────────────────────────────────────

async def is_sl_placed(symbol: str) -> bool:
    """Bu sembol icin acik poz i̇cin SL algo emri koyuldu mu?"""
    try:
        r = await get_redis()
        v = await r.get(_key(KEY_SL_PLACED, symbol))
        return v is not None
    except Exception:
        return False


async def mark_sl_placed(symbol: str) -> None:
    """PLACE_SL basariyla islendi — flag koy (2. PLACE_SL alert i̇çin idempotency)."""
    try:
        r = await get_redis()
        await r.set(_key(KEY_SL_PLACED, symbol), "1", ex=TTL_SL_FLAG)
    except Exception as e:
        await log.awarning("webhook_tracker_mark_sl_failed", symbol=symbol, error=str(e))


async def clear_sl_flag(symbol: str) -> None:
    """Pozisyon kapandi — SL flag'ini sil ki yeni poz i̇cin tekrar kabul edilsin."""
    try:
        r = await get_redis()
        await r.delete(_key(KEY_SL_PLACED, symbol))
    except Exception as e:
        await log.awarning("webhook_tracker_clear_sl_failed", symbol=symbol, error=str(e))


# ── Position meta (fill sonrasi ihtiyac icin) ───────────────────────────

async def set_pos_meta(symbol: str, meta: dict[str, Any]) -> None:
    """Fill sonrasi poz bilgisi (side, entry, qty, tp_algo_id vb.) kaydet."""
    try:
        r = await get_redis()
        await r.set(_key(KEY_POS_META, symbol), json.dumps(meta), ex=TTL_POS_META)
    except Exception as e:
        await log.awarning("webhook_tracker_set_meta_failed", symbol=symbol, error=str(e))


async def get_pos_meta(symbol: str) -> dict[str, Any] | None:
    """Acik poz meta bilgisini getir."""
    try:
        r = await get_redis()
        raw = await r.get(_key(KEY_POS_META, symbol))
        if raw is None:
            return None
        return json.loads(raw)
    except Exception:
        return None


async def clear_pos_meta(symbol: str) -> None:
    """Pozisyon kapandi — meta'yi sil."""
    try:
        r = await get_redis()
        await r.delete(_key(KEY_POS_META, symbol))
    except Exception as e:
        await log.awarning("webhook_tracker_clear_meta_failed", symbol=symbol, error=str(e))


# ── Bulk cleanup (poz tamamen kapandiginda) ─────────────────────────────

async def clear_all_state(symbol: str) -> None:
    """Poz kapanis eventinde tum webhook state'ini temizle:
    pending limit, SL flag, poz meta.
    """
    await clear_pending_limit(symbol)
    await clear_sl_flag(symbol)
    await clear_pos_meta(symbol)
    await log.ainfo("webhook_tracker_cleared_all", symbol=symbol)
