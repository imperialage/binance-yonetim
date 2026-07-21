"""Webhook order poller — TF-aware bar close REST fallback.

Binance user-data WebSocket ORDER_TRADE_UPDATE event'leri sessizce dusunce
webhook LIMIT emirlerin fill/cancel durumunu REST uzerinden kontrol eder.

TF-AWARE POLLING (kullanici tercihi):
  Her sembol icin Pine'dan gelen TF'e bakariz. Bar close'una ~10 saniye kala
  Binance'a GET /fapi/v1/order sorusu atariz. Boylece:
    - Emir bar boyunca acik kalir (LIMIT fill bar icinde herhangi bir tick'te)
    - Bar close'una yakin kesin sonucu ogreniriz
    - Sonraki bar'da yeni PLACE_LIMIT gelirse zaten backend eski emri iptal
      eder — polling sadece fill kacirmayi engeller

Ornek: ETHUSDT 15dk grafik icin bar close'lar UTC 00:15, 00:30, 00:45, 01:00.
  Poller her 5sn'de tick, bar close'a 3-15 sn kaldiginda o sembol icin sorgu.
  Ayni bar_id icin 2. sorgu skip (Redis flag).

Pozisyon kapama tespit (SL/TP tetigi veya manuel close): 30sn'de bir
webhook_sl_placed:* keys'i tara, positionRisk kontrol et. Poz yoksa
Redis state tam temizle.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from app.modules.redis_client import get_redis
from app.utils.logging import get_logger

log = get_logger(__name__)

# Ana loop tick araligi. Kucuk = daha responsive ama daha cok Redis scan.
POLL_INTERVAL = 5

# Bar close'a bu kadar saniye kaldiginda Binance'a sorgu at.
# Ornek: WINDOW=15 → bar close'a 0-15sn kaldi ise poll et.
BAR_CLOSE_WINDOW = 15

# Pozisyon kapanma kontrol araligi (POLL_INTERVAL katı).
POSITION_CHECK_INTERVAL = 30

# Ayni bar icin ikinci poll'u engelleyen flag TTL (bar_id her bar unique).
POLLED_FLAG_TTL = 2 * 3600  # 2 saat

# Ana loop stop signal
_task: asyncio.Task[None] | None = None


def _tf_to_seconds(tf: str) -> int:
    """Pine TF stringini saniyeye cevir.
    Formatlar: "15", "60", "5m", "15m", "1h", "4h", "D".
    Sadece rakam ise DAKIKA kabul (Pine {{interval}} default format).
    """
    if not tf:
        return 0
    tf = str(tf).strip().lower()
    try:
        if tf.endswith("m"):
            return int(tf[:-1]) * 60
        if tf.endswith("h"):
            return int(tf[:-1]) * 3600
        if tf.endswith("d") or tf == "d":
            return int(tf[:-1] or "1") * 86400
        # Sadece rakam ise dakika
        return int(tf) * 60
    except (ValueError, TypeError):
        return 0


def _seconds_until_bar_close(tf_sec: int, now_ts: int | None = None) -> int:
    """Bar close'una kac saniye kaldi (Binance UTC epoch bazli)."""
    if tf_sec <= 0:
        return -1
    if now_ts is None:
        now_ts = int(time.time())
    next_close = ((now_ts // tf_sec) + 1) * tf_sec
    return next_close - now_ts


def _current_bar_id(tf_sec: int, now_ts: int | None = None) -> int:
    """Su anki bar'in acilis timestamp'i (ms)."""
    if tf_sec <= 0:
        return 0
    if now_ts is None:
        now_ts = int(time.time())
    return (now_ts // tf_sec) * tf_sec * 1000


def _rest_order_to_ws_event(rest_order: dict) -> dict:
    """REST /fapi/v1/order response'unu WS ORDER_TRADE_UPDATE format'ina cevir.

    handle_fill_event WS event format bekliyor. REST icin adapter — mevcut
    fill handler'i reuse ederiz.
    """
    return {
        "i": rest_order.get("orderId"),
        "s": rest_order.get("symbol", ""),
        "X": rest_order.get("status", ""),
        "S": rest_order.get("side", ""),
        "ot": rest_order.get("origType") or rest_order.get("type", ""),
        "c": rest_order.get("clientOrderId", ""),
        "z": rest_order.get("executedQty", "0"),
        "ap": rest_order.get("avgPrice", "0"),
        "q": rest_order.get("origQty", "0"),
        "R": rest_order.get("reduceOnly", False),
        "cp": rest_order.get("closePosition", False),
        "rp": rest_order.get("realizedProfit", "0"),
    }


async def _polled_this_bar(symbol: str, bar_key: int) -> bool:
    """Ayni sembol + bar icin 2. poll engellemek icin Redis flag check."""
    try:
        r = await get_redis()
        key = f"webhook_poll_flag:{symbol}:{bar_key}"
        ok = await r.set(key, "1", nx=True, ex=POLLED_FLAG_TTL)
        # ok=True → set edildi (ilk), False → zaten var (dup)
        return not bool(ok)
    except Exception:
        return False  # fail-open: sorgu at


async def _process_one_pending(symbol: str) -> None:
    """Bir sembol icin bar close yakinsa order status kontrol."""
    from app.modules import webhook_order_tracker as tracker
    from app.modules.binance_client import get_order_status
    from app.routers.st_webhook import handle_fill_event

    pending = await tracker.get_pending_limit(symbol)
    if pending is None:
        return

    tf = pending.get("tf", "")
    tf_sec = _tf_to_seconds(tf)
    if tf_sec <= 0:
        # TF yoksa (eski format?) — her tick sorgu at (fallback davranis)
        remaining = 0
    else:
        remaining = _seconds_until_bar_close(tf_sec)

    # Bar close'a BAR_CLOSE_WINDOW saniyeden fazla varsa skip
    if tf_sec > 0 and remaining > BAR_CLOSE_WINDOW:
        return

    # Dedupe: bu bar icin zaten sorgu attıysa skip
    bar_key = _current_bar_id(tf_sec)
    if tf_sec > 0 and await _polled_this_bar(symbol, bar_key):
        return

    order_id = pending.get("orderId")
    if not order_id:
        await tracker.clear_pending_limit(symbol)
        return

    try:
        rest_order = await get_order_status(symbol, int(order_id))
    except Exception as e:
        msg = str(e).lower()
        if "-2013" in msg or "does not exist" in msg or "-2011" in msg:
            await log.ainfo("poller_order_gone_cleanup",
                            symbol=symbol, order_id=order_id)
            await tracker.clear_pending_limit(symbol)
        else:
            await log.awarning("poller_get_status_failed",
                               symbol=symbol, order_id=order_id, error=str(e))
        return

    status = rest_order.get("status", "")
    await log.ainfo("poller_checked", symbol=symbol, order_id=order_id,
                    status=status, tf=tf, remaining_s=remaining)

    if status == "FILLED":
        # WS format'ina cevir + handle_fill_event tetikle
        ws_event = _rest_order_to_ws_event(rest_order)
        try:
            await handle_fill_event(ws_event)
            await log.ainfo("poller_fill_dispatched", symbol=symbol,
                            order_id=order_id, avg_price=rest_order.get("avgPrice"))
        except Exception as e:
            await log.aerror("poller_handle_fill_error", symbol=symbol, error=str(e))

    elif status in ("CANCELED", "EXPIRED", "REJECTED"):
        await log.ainfo("poller_order_terminal_cleanup",
                        symbol=symbol, order_id=order_id, status=status)
        await tracker.clear_pending_limit(symbol)

    # PARTIALLY_FILLED / NEW: devam, bir sonraki bar close'a tekrar kontrol


async def _poll_pending_orders() -> None:
    """webhook_limit:* scan et, her sembol icin _process_one_pending."""
    try:
        r = await get_redis()
        keys = []
        async for k in r.scan_iter(match="webhook_limit:*", count=100):
            keys.append(k)
    except Exception as e:
        await log.awarning("poller_scan_failed", error=str(e))
        return

    for key in keys:
        try:
            symbol = key.split(":", 1)[1] if ":" in key else ""
            if not symbol:
                continue
            await _process_one_pending(symbol)
        except Exception as e:
            await log.awarning("poller_process_failed", key=key, error=str(e))


async def _poll_positions() -> None:
    """webhook_sl_placed:* keys tara, poz kapanmis mi kontrol et.

    Poz kapandiginda (positionAmt=0) TUM webhook state'i temizle. Bu manuel
    poz kapama, SL/TP tetiklemesi vs. tum durumlari kapsar.
    """
    from app.modules import webhook_order_tracker as tracker
    from app.modules.binance_client import get_position_risk

    try:
        r = await get_redis()
        keys = []
        async for k in r.scan_iter(match="webhook_sl_placed:*", count=100):
            keys.append(k)
    except Exception as e:
        await log.awarning("poller_pos_scan_failed", error=str(e))
        return

    for key in keys:
        try:
            symbol = key.split(":", 1)[1] if ":" in key else ""
            if not symbol:
                continue

            positions = await get_position_risk(symbol)
            pos_amt = 0.0
            for p in positions:
                if p.get("symbol") == symbol:
                    pos_amt = float(p.get("positionAmt", 0))
                    break

            if pos_amt == 0:
                await log.ainfo("poller_position_closed_cleanup", symbol=symbol)
                await tracker.clear_all_state(symbol)
        except Exception as e:
            await log.awarning("poller_pos_check_failed", key=key, error=str(e))


async def _poller_loop() -> None:
    """Ana loop — POLL_INTERVAL sn'de bir tick.
    Her tick order status check (TF-aware). Her N tick position check.
    """
    await log.ainfo("webhook_poller_starting",
                    poll_interval=POLL_INTERVAL,
                    bar_close_window=BAR_CLOSE_WINDOW,
                    pos_check_interval=POSITION_CHECK_INTERVAL)

    tick = 0
    pos_check_every = max(1, POSITION_CHECK_INTERVAL // POLL_INTERVAL)

    try:
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            tick += 1

            try:
                await _poll_pending_orders()
            except Exception as e:
                await log.awarning("poller_orders_tick_failed", error=str(e))

            if tick % pos_check_every == 0:
                try:
                    await _poll_positions()
                except Exception as e:
                    await log.awarning("poller_positions_tick_failed", error=str(e))

    except asyncio.CancelledError:
        await log.ainfo("webhook_poller_stopped")


def start_webhook_poller() -> asyncio.Task[None]:
    """Poller task'ini baslat (idempotent)."""
    global _task
    if _task is not None and not _task.done():
        return _task
    _task = asyncio.create_task(_poller_loop())
    return _task


async def stop_webhook_poller() -> None:
    """Poller task'ini durdur."""
    global _task
    if _task is not None and not _task.done():
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        _task = None
