"""Flip watcher — HTF ters donus tespiti + backend mudahale.

Pine indikatoru PLACE_LIMIT gonderdi, backend fill etti. Normalde fill bar
kapanisinda Pine PLACE_SL alert atar. Ancak HTF renk fill sonrasi doner ve
Pine yeni pozu SL ile korumaz — poz yalniz kalir.

Bu modul her fill sonrasi bir timer arm eder:
  - bekle: fill_bar_close + settings.flip_check_margin_ms
  - kontrol: webhook_sl_placed flag TRUE mi?
    - EVET: normal akis, disarm
    - HAYIR: FLIP MODE — mevcut algo emirleri iptal + mini-TP (flip_exit_pct)
             + SL (flip_sl_pct) koy

Pine tarafina dokunmadan calisir. Restart resilience: Redis-backed state,
startup'ta restore_all() ile kaldigi yerden devam eder.
"""

from __future__ import annotations

import asyncio
import time

from app.config import settings
from app.modules import webhook_order_tracker as tracker
from app.utils.logging import get_logger

log = get_logger(__name__)

# Symbol -> asyncio.Task (per-symbol tek watcher)
_tasks: dict[str, asyncio.Task] = {}

# Pine timeframe.period → ms
_TF_MS: dict[str, int] = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
    # Pine numeric-only (timeframe.period ornek: "60", "240")
    "1": 60_000, "3": 180_000, "5": 300_000, "15": 900_000, "30": 1_800_000,
    "60": 3_600_000, "120": 7_200_000, "240": 14_400_000, "360": 21_600_000,
    "480": 28_800_000, "720": 43_200_000, "D": 86_400_000, "1D": 86_400_000,
}


def _tf_to_ms(tf: str) -> int:
    """Pine TF stringi → ms. Bilinmeyen icin 15m fallback."""
    if not tf:
        return 15 * 60_000
    return _TF_MS.get(tf.strip().lower()) or _TF_MS.get(tf.strip()) or 15 * 60_000


async def arm(
    symbol: str,
    *,
    fill_bar_id_ms: int,
    tf: str,
    entry: float,
    side: str,
    qty: float,
) -> None:
    """Fill sonrasi flip check timer'i baslat.

    Args:
        symbol: Binance sembol (BTCUSDT).
        fill_bar_id_ms: Pine barin acilis time (ms) — pending Redis'ten gelir.
        tf: Pine indikator TF ("60", "15m" vs).
        entry: Avg fill fiyati.
        side: Poz yonu — Pine payload side (BUY=LONG, SELL=SHORT).
        qty: Filled quantity (mutlak).
    """
    if not settings.flip_mode_enabled:
        return

    await tracker.set_flip_watch(symbol, {
        "fill_bar_id_ms": int(fill_bar_id_ms),
        "tf": tf,
        "entry": float(entry),
        "side": side.upper(),
        "qty": float(qty),
        "armed_at": int(time.time() * 1000),
    })

    _spawn_task(symbol)
    await log.ainfo("flip_watch_armed", symbol=symbol, tf=tf, entry=entry,
                    side=side, qty=qty, bar_id_ms=fill_bar_id_ms)


def _spawn_task(symbol: str) -> None:
    """Existing task varsa iptal + yeni task olustur."""
    sym = symbol.upper()
    old = _tasks.pop(sym, None)
    if old and not old.done():
        old.cancel()
    _tasks[sym] = asyncio.create_task(_watch(sym), name=f"flip_watch_{sym}")


async def _watch(symbol: str) -> None:
    """Bar close + margin bekle, SL geldiyse cik, gelmediyse flip mode aktif."""
    try:
        meta = await tracker.get_flip_watch(symbol)
        if not meta:
            return

        fill_bar_id_ms = int(meta["fill_bar_id_ms"])
        tf = str(meta["tf"])
        entry = float(meta["entry"])
        side = str(meta["side"])
        qty = float(meta["qty"])

        tf_ms = _tf_to_ms(tf)
        bar_close_ms = fill_bar_id_ms + tf_ms
        check_at_ms = bar_close_ms + settings.flip_check_margin_ms
        now_ms = int(time.time() * 1000)
        sleep_s = (check_at_ms - now_ms) / 1000.0

        if sleep_s > 0:
            await asyncio.sleep(sleep_s)

        # 1) SL geldi mi?
        if await tracker.is_sl_placed(symbol):
            await log.ainfo("flip_watch_sl_ok", symbol=symbol)
            await tracker.clear_flip_watch(symbol)
            return

        # 2) Poz hala acik mi?
        from app.modules.binance_client import get_position_risk
        positions = await get_position_risk(symbol)
        pos_amt = 0.0
        for p in positions:
            if p.get("symbol") == symbol:
                pos_amt = float(p.get("positionAmt", 0))
                break

        if pos_amt == 0:
            await log.ainfo("flip_watch_no_position", symbol=symbol)
            await tracker.clear_flip_watch(symbol)
            return

        # 3) FLIP MODE — SL yok + poz acik = HTF ters dondu
        await log.awarning("flip_watch_triggered", symbol=symbol,
                           entry=entry, side=side, pos_amt=pos_amt)
        await _activate_flip_mode(symbol, entry, side, abs(pos_amt))
        await tracker.clear_flip_watch(symbol)

    except asyncio.CancelledError:
        # disarm/shutdown — normal
        raise
    except Exception as e:
        await log.aerror("flip_watch_error", symbol=symbol, error=str(e))


async def _activate_flip_mode(symbol: str, entry: float, side: str, qty: float) -> None:
    """Mevcut algo emirleri iptal + mini-TP (flip_exit_pct) + SL (flip_sl_pct) koy.

    LONG (side=BUY):  mini-TP = entry*(1+flip%), SL = entry*(1-sl%), close_side=SELL
    SHORT (side=SELL): mini-TP = entry*(1-flip%), SL = entry*(1+sl%), close_side=BUY
    """
    from app.modules import webhook_order_tracker as _tracker
    from app.modules.binance_client import (
        cancel_all_open_orders,
        place_stop_market_instant,
        place_take_profit_market_order,
        round_price,
    )
    from app.modules.trade_executor import get_exchange_info_cached

    # 1) Mevcut algo emirlerini iptal (eski TP dahil)
    try:
        await cancel_all_open_orders(symbol)
    except Exception as e:
        await log.awarning("flip_mode_cancel_failed", symbol=symbol, error=str(e))

    # 2) Fiyatlari hesapla + tick round
    info = await get_exchange_info_cached(symbol)
    tick_size = info["priceFilter"]["tickSize"]

    flip_pct = settings.flip_exit_pct
    sl_pct = settings.flip_sl_pct

    if side.upper() == "BUY":
        mini_tp_raw = entry * (1 + flip_pct)
        sl_raw = entry * (1 - sl_pct)
        close_side = "SELL"
    else:
        mini_tp_raw = entry * (1 - flip_pct)
        sl_raw = entry * (1 + sl_pct)
        close_side = "BUY"

    mini_tp = round_price(mini_tp_raw, tick_size)
    sl_px = round_price(sl_raw, tick_size)

    # 3) Mini-TP
    tp_ok = False
    try:
        await place_take_profit_market_order(symbol, close_side, qty, float(mini_tp))
        tp_ok = True
        await log.ainfo("flip_mode_mini_tp_placed", symbol=symbol,
                        side=close_side, price=float(mini_tp),
                        pct=round(flip_pct * 100, 4))
    except Exception as e:
        await log.aerror("flip_mode_mini_tp_failed", symbol=symbol, error=str(e))

    # 4) SL
    sl_ok = False
    try:
        await place_stop_market_instant(symbol, close_side, qty, float(sl_px))
        await _tracker.mark_sl_placed(symbol)  # gec gelen PLACE_SL alerti no-op olsun
        sl_ok = True
        await log.ainfo("flip_mode_sl_placed", symbol=symbol,
                        side=close_side, price=float(sl_px),
                        pct=round(sl_pct * 100, 4))
    except Exception as e:
        await log.aerror("flip_mode_sl_failed", symbol=symbol, error=str(e))

    await log.ainfo("flip_mode_activated", symbol=symbol, entry=entry, side=side,
                    qty=qty, mini_tp=float(mini_tp), sl=float(sl_px),
                    tp_ok=tp_ok, sl_ok=sl_ok)


async def disarm(symbol: str) -> None:
    """PLACE_SL geldi ya da poz kapandi — flip watch iptal."""
    sym = symbol.upper()
    task = _tasks.pop(sym, None)
    if task and not task.done():
        task.cancel()
    await tracker.clear_flip_watch(symbol)
    await log.ainfo("flip_watch_disarmed", symbol=sym)


async def restore_all() -> None:
    """Startup: Redis'teki tum flip watch'lar icin task spawn (restart resilience)."""
    try:
        symbols = await tracker.list_flip_watches()
        for sym in symbols:
            _spawn_task(sym)
            await log.ainfo("flip_watch_restored", symbol=sym)
        if symbols:
            await log.ainfo("flip_watch_restored_count", n=len(symbols))
    except Exception as e:
        await log.awarning("flip_watch_restore_failed", error=str(e))


async def stop_all() -> None:
    """Shutdown — tum task'lari iptal et."""
    for sym, task in list(_tasks.items()):
        if not task.done():
            task.cancel()
    _tasks.clear()
