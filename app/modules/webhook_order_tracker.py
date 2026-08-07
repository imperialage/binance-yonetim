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
KEY_FLIP_WATCH = "webhook_flip_watch:{sym}"
KEY_FILL_BAR_CHECK = "webhook_fill_bar_check:{sym}"  # fill bar close HTF karari icin bekle
KEY_FLIP_DECIDED = "webhook_flip_decided:{sym}"      # bar close karari verildi (sonraki HTF_STATUS ignore)
KEY_DEFERRED = "webhook_deferred:{sym}"              # v3.8: Pine 2 sonrasi dogru yon LIMIT emri
KEY_EMERGENCY_SL = "webhook_emergency_sl:{sym}"      # v3.10: Fill aninda konulan gecici SL algo_id
KEY_SL_LOCK = "webhook_sl_lock:{sym}"                # v3.14: SL yerlestirme atomic lock (race koruma)

# TTL değerleri (saniye)
TTL_LIMIT = 6 * 60 * 60      # 6 saat — dolmayan pending emri unut
TTL_SL_FLAG = 24 * 60 * 60   # 24 saat — poz kapanisinda silinir zaten
TTL_POS_META = 24 * 60 * 60  # 24 saat
TTL_FLIP_WATCH = 6 * 60 * 60 # 6 saat — bar close + margin cok gecmeden temizlenir
TTL_FILL_BAR_CHECK = 6 * 60 * 60  # 6 saat — poz suresince
TTL_FLIP_DECIDED = 24 * 60 * 60  # 24 saat — poz kapanisinda silinir
TTL_DEFERRED = 30 * 60       # 30 dakika — 2 bar 15m TF, eski deferred gecersiz
TTL_EMERGENCY_SL = 6 * 60 * 60  # 6 saat — poz suresince yeterli


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


# ── Flip watch state (fill sonrasi SL bekle, gelmezse mini-TP + SL koy) ─

async def set_flip_watch(symbol: str, meta: dict[str, Any]) -> None:
    """Fill sonrasi flip check meta kaydet.

    Beklenen alanlar: fill_bar_id_ms, tf, entry, side, qty, armed_at.
    """
    try:
        r = await get_redis()
        await r.set(_key(KEY_FLIP_WATCH, symbol), json.dumps(meta), ex=TTL_FLIP_WATCH)
    except Exception as e:
        await log.awarning("webhook_tracker_set_flipwatch_failed", symbol=symbol, error=str(e))


async def get_flip_watch(symbol: str) -> dict[str, Any] | None:
    try:
        r = await get_redis()
        raw = await r.get(_key(KEY_FLIP_WATCH, symbol))
        if raw is None:
            return None
        return json.loads(raw)
    except Exception:
        return None


async def clear_flip_watch(symbol: str) -> None:
    try:
        r = await get_redis()
        await r.delete(_key(KEY_FLIP_WATCH, symbol))
    except Exception as e:
        await log.awarning("webhook_tracker_clear_flipwatch_failed", symbol=symbol, error=str(e))


async def list_flip_watches() -> list[str]:
    """Redis'teki tum flip watch key'lerinden sembol listesi (restart resilience)."""
    try:
        r = await get_redis()
        pattern = KEY_FLIP_WATCH.format(sym="*")
        symbols: list[str] = []
        async for k in r.scan_iter(match=pattern, count=100):
            key = k.decode() if isinstance(k, bytes) else k
            if ":" in key:
                symbols.append(key.split(":", 1)[1])
        return symbols
    except Exception as e:
        await log.awarning("webhook_tracker_list_flipwatches_failed", error=str(e))
        return []


# ── Fill bar close karar bekleme state (v3.7 yeni) ───────────────────────
# Fill event handler bunu set eder: "fill oldu, fill_bar_id sonrasi HTF_STATUS bekle,
# bir kere karar ver, sonra sil". HTF_STATUS handler flag'i sil, boylece sonraki
# HTF_STATUS'lar ignore edilir.

async def set_fill_bar_check(symbol: str, fill_bar_id_ms: int) -> None:
    """Fill oldu — fill_bar_id_ms sonrasi ilk HTF_STATUS'ta karar verilecek."""
    try:
        r = await get_redis()
        await r.set(_key(KEY_FILL_BAR_CHECK, symbol), str(fill_bar_id_ms), ex=TTL_FILL_BAR_CHECK)
    except Exception as e:
        await log.awarning("webhook_tracker_set_fillbarcheck_failed", symbol=symbol, error=str(e))


async def get_fill_bar_check(symbol: str) -> int | None:
    """Fill_bar_id_ms doner ya da None (karar zaten verildi/poz yok)."""
    try:
        r = await get_redis()
        v = await r.get(_key(KEY_FILL_BAR_CHECK, symbol))
        return int(v) if v is not None else None
    except Exception:
        return None


async def clear_fill_bar_check(symbol: str) -> None:
    try:
        r = await get_redis()
        await r.delete(_key(KEY_FILL_BAR_CHECK, symbol))
    except Exception as e:
        await log.awarning("webhook_tracker_clear_fillbarcheck_failed", symbol=symbol, error=str(e))


# ── Flip decided flag — sonraki HTF_STATUS'lar icin ignore ───────────────

async def mark_flip_decided(symbol: str) -> None:
    """Bar close karari verildi — sonraki HTF_STATUS'lari ignore."""
    try:
        r = await get_redis()
        await r.set(_key(KEY_FLIP_DECIDED, symbol), "1", ex=TTL_FLIP_DECIDED)
    except Exception as e:
        await log.awarning("webhook_tracker_mark_decided_failed", symbol=symbol, error=str(e))


async def is_flip_decided(symbol: str) -> bool:
    try:
        r = await get_redis()
        v = await r.get(_key(KEY_FLIP_DECIDED, symbol))
        return v is not None
    except Exception:
        return False


async def clear_flip_decided(symbol: str) -> None:
    try:
        r = await get_redis()
        await r.delete(_key(KEY_FLIP_DECIDED, symbol))
    except Exception as e:
        await log.awarning("webhook_tracker_clear_decided_failed", symbol=symbol, error=str(e))


# ── Deferred Entry Queue (v3.8) ──────────────────────────────────────────
# Pine 2 protokolu uygulandiginda ters pozdan cikmak icin mini-TP + SL yaninda
# dogru yon LIMIT emri koyariz. Fiyat mini-TP fiyatinin %0.1 yakininda.
# Fill oldugunda POSITION_STATUS ile Pine'in TP/SL fiyatlari uygulanir.

async def set_deferred_entry(symbol: str, meta: dict[str, Any]) -> None:
    """Deferred entry meta kaydet.
    Beklenen alanlar: side, price, tp_price, sl_price, source_bar_id,
                     order_id (Binance LIMIT emir ID'si), saved_at.
    """
    try:
        r = await get_redis()
        await r.set(_key(KEY_DEFERRED, symbol), json.dumps(meta), ex=TTL_DEFERRED)
    except Exception as e:
        await log.awarning("webhook_tracker_set_deferred_failed", symbol=symbol, error=str(e))


async def get_deferred_entry(symbol: str) -> dict[str, Any] | None:
    try:
        r = await get_redis()
        raw = await r.get(_key(KEY_DEFERRED, symbol))
        if raw is None:
            return None
        return json.loads(raw)
    except Exception:
        return None


async def clear_deferred_entry(symbol: str) -> None:
    try:
        r = await get_redis()
        await r.delete(_key(KEY_DEFERRED, symbol))
    except Exception as e:
        await log.awarning("webhook_tracker_clear_deferred_failed", symbol=symbol, error=str(e))


# ── Emergency SL (v3.10) — fill aninda konulan gecici %2 SL ─────────────

async def set_emergency_sl(symbol: str, algo_id: int | str | None, sl_price: float) -> None:
    """Fill aninda konulan gecici SL algo_id'yi kaydet.
    Pine PLACE_SL geldiginde bu algo_id iptal edilir + Pine SL koyulur."""
    if algo_id is None:
        return
    try:
        r = await get_redis()
        await r.set(_key(KEY_EMERGENCY_SL, symbol),
                    json.dumps({"algo_id": str(algo_id), "sl_price": float(sl_price)}),
                    ex=TTL_EMERGENCY_SL)
    except Exception as e:
        await log.awarning("webhook_tracker_set_emergency_sl_failed", symbol=symbol, error=str(e))


async def get_emergency_sl(symbol: str) -> dict | None:
    try:
        r = await get_redis()
        raw = await r.get(_key(KEY_EMERGENCY_SL, symbol))
        if raw is None:
            return None
        return json.loads(raw)
    except Exception:
        return None


async def clear_emergency_sl(symbol: str) -> None:
    try:
        r = await get_redis()
        await r.delete(_key(KEY_EMERGENCY_SL, symbol))
    except Exception as e:
        await log.awarning("webhook_tracker_clear_emergency_sl_failed", symbol=symbol, error=str(e))


# ── SL Lock (v3.14) — atomic race koruma ────────────────────────────────
# POSITION_STATUS ve PLACE_SL handler'lari ayni anda gelebilir.
# openAlgoOrders sorgu → POST arasindaki ~50ms window'da ikisi de "SL yok"
# gorup ikili koyuyordu. Redis SET NX (set if not exists) ile atomik lock.

async def try_acquire_sl_lock(symbol: str, ttl_sec: int = 10) -> bool:
    """SL lock al. Basarili ise True, biri bekliyorsa False."""
    try:
        r = await get_redis()
        result = await r.set(_key(KEY_SL_LOCK, symbol), "1", nx=True, ex=ttl_sec)
        return result is True
    except Exception as e:
        await log.awarning("webhook_tracker_sl_lock_failed", symbol=symbol, error=str(e))
        return True  # lock alınamadıysa serbest bırak (fallback)


async def release_sl_lock(symbol: str) -> None:
    try:
        r = await get_redis()
        await r.delete(_key(KEY_SL_LOCK, symbol))
    except Exception as e:
        await log.awarning("webhook_tracker_sl_lock_release_failed", symbol=symbol, error=str(e))


# ── Bulk cleanup (poz tamamen kapandiginda) ─────────────────────────────

async def clear_all_state(symbol: str) -> None:
    """Poz kapanis eventinde tum webhook state'ini temizle:
    pending limit, SL flag, poz meta, flip watch, fill bar check, flip decided,
    deferred entry, emergency SL.
    """
    await clear_pending_limit(symbol)
    await clear_sl_flag(symbol)
    await clear_pos_meta(symbol)
    await clear_flip_watch(symbol)
    await clear_fill_bar_check(symbol)
    await clear_flip_decided(symbol)
    await clear_deferred_entry(symbol)
    await clear_emergency_sl(symbol)
    await log.ainfo("webhook_tracker_cleared_all", symbol=symbol)
