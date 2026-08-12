"""Pine Chaser — ters pozdan market ile ciktiktan sonra Pine 1'in dogru yon
TP hedefine kar mesafesi olusursa market entry yap.

Kural (kullanici tanimi):
  - Ters pozdan market ile çıktık (POSITION_STATUS mismatch veya CANCEL)
  - Pine 1'in dogru yon TP fiyati belli
  - Her tick fiyat kontrol: (Pine TP - mark) / mark >= min_profit_pct (LONG)
    veya tersi (SHORT)
  - Sart TRUE olan ilk anda MARKET ENTRY + Pine TP + Pine SL algo
  - Chaser tek durur: PINE_EXIT alerti (Pine backtest'te poz kapandi)

6 katman guvenlik:
  1. Atomik Redis exec lock (chaser tick race koruma)
  2. Pre-check (poz zaten var mi, balance, notional, fiyat re-check)
  3. reduceOnly (market ENTRY icin false, ama ters cikis market EXIT icin true)
  4. Verify-after-place (Binance state doğrulama, 3 retry exponential backoff)
  5. TP+SL algo_lock (mevcut v3.15 lock kullan)
  6. Failure telemetry (retry ve critical log)
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

from app.config import settings
from app.modules import webhook_order_tracker as tracker
from app.utils.logging import get_logger

log = get_logger(__name__)

# Symbol -> asyncio.Task (per-symbol tek watcher)
_tasks: dict[str, asyncio.Task] = {}


def _spawn_task(symbol: str) -> None:
    sym = symbol.upper()
    old = _tasks.pop(sym, None)
    if old and not old.done():
        old.cancel()
    _tasks[sym] = asyncio.create_task(_watch(sym), name=f"chaser_{sym}")


async def arm(
    symbol: str,
    *,
    pine_side: str,       # "LONG" veya "SHORT"
    pine_tp: float,
    pine_sl: float,
    chart_tf_ms: int = 900_000,  # 15m default
) -> None:
    """Chaser başlat. Var olan chaser varsa overwrite eder."""
    if not settings.pine_chaser_enabled:
        return
    await tracker.set_chaser(symbol, {
        "pine_side": pine_side.upper(),
        "pine_tp": float(pine_tp),
        "pine_sl": float(pine_sl),
        "chart_tf_ms": int(chart_tf_ms),
        "started_at_ms": int(time.time() * 1000),
        "min_profit_pct": settings.pine_chaser_min_profit_pct,
    })
    _spawn_task(symbol)
    await log.ainfo("chaser_armed", symbol=symbol,
                    pine_side=pine_side, pine_tp=pine_tp, pine_sl=pine_sl)


async def update_pine_targets(symbol: str, pine_side: str, pine_tp: float, pine_sl: float) -> None:
    """POSITION_STATUS same-side geldiğinde Pine hedefleri güncelle."""
    meta = await tracker.get_chaser(symbol)
    if not meta:
        return
    if meta.get("pine_side") != pine_side.upper():
        return  # farkli yön — caller disarm + arm yapmalı
    meta["pine_tp"] = float(pine_tp)
    meta["pine_sl"] = float(pine_sl)
    await tracker.set_chaser(symbol, meta)
    await log.ainfo("chaser_updated", symbol=symbol,
                    pine_tp=pine_tp, pine_sl=pine_sl)


async def disarm(symbol: str, reason: str = "manual") -> None:
    """Chaser durdur (PINE_EXIT, market entry basarili, vs.)."""
    sym = symbol.upper()
    task = _tasks.pop(sym, None)
    if task and not task.done():
        task.cancel()
    await tracker.clear_chaser(symbol)
    await tracker.release_chaser_exec_lock(symbol)
    await log.ainfo("chaser_disarmed", symbol=sym, reason=reason)


async def _watch(symbol: str) -> None:
    """Chaser watcher — tick loop, market entry firsat kollar."""
    try:
        while True:
            meta = await tracker.get_chaser(symbol)
            if not meta:
                return  # chaser silinmis
            # Fiyat + kar mesafesi
            try:
                await _tick_check(symbol, meta)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await log.awarning("chaser_tick_error", symbol=symbol, error=str(e))
            await asyncio.sleep(settings.pine_chaser_tick_sec)
    except asyncio.CancelledError:
        raise


async def _tick_check(symbol: str, meta: dict) -> None:
    """Tek tick: fiyat kar mesafede mi kontrol + market entry."""
    from app.modules.price_stream import get_live_price

    mark = get_live_price(symbol)
    if mark is None or mark <= 0:
        return

    pine_side = str(meta["pine_side"]).upper()
    pine_tp = float(meta["pine_tp"])
    pine_sl = float(meta["pine_sl"])
    min_profit = float(meta.get("min_profit_pct", 0.003))

    # Potansiyel kar
    if pine_side == "LONG":
        potential = (pine_tp - mark) / mark
    else:  # SHORT
        potential = (mark - pine_tp) / mark

    if potential < min_profit:
        return  # fırsat yok, bekle

    # ── FIRSAT VAR — atomik lock + market entry ──
    lock_ok = await tracker.try_acquire_chaser_exec_lock(symbol)
    if not lock_ok:
        return  # baska tick zaten isliyor

    try:
        await _execute_market_entry(symbol, meta, mark, potential)
    finally:
        await tracker.release_chaser_exec_lock(symbol)


async def _execute_market_entry(
    symbol: str, meta: dict, mark_at_signal: float, potential_at_signal: float
) -> None:
    """MARKET entry pipeline — pre-check + place + verify + TP/SL algo."""
    from app.config import settings as _settings
    from app.modules.binance_client import (
        get_available_balance, get_position_risk, get_total_wallet_balance,
        place_market_order, place_stop_market_instant, place_take_profit_market_order,
        round_price, round_step_size,
    )
    from app.modules.indicator_settings_store import get_settings_or_defaults
    from app.modules.trade_executor import get_exchange_info_cached

    pine_side = str(meta["pine_side"]).upper()
    pine_tp = float(meta["pine_tp"])
    pine_sl = float(meta["pine_sl"])
    is_long = pine_side == "LONG"
    order_side = "BUY" if is_long else "SELL"
    close_side = "SELL" if is_long else "BUY"

    # ── Pre-check ──
    positions = await get_position_risk(symbol)
    pos_amt = 0.0
    for p in positions:
        if p.get("symbol") == symbol:
            pos_amt = float(p.get("positionAmt", 0))
            break
    if pos_amt != 0:
        # Poz zaten var — chaser boşuna denemesin
        await log.ainfo("chaser_position_exists_skip", symbol=symbol,
                        pos_amt=pos_amt, reason="external_entry")
        await disarm(symbol, reason="position_exists")
        return

    # Fiyat re-check (tick arası kayma)
    from app.modules.price_stream import get_live_price
    mark_now = get_live_price(symbol)
    if mark_now is None or mark_now <= 0:
        return
    if is_long:
        potential_now = (pine_tp - mark_now) / mark_now
    else:
        potential_now = (mark_now - pine_tp) / mark_now
    if potential_now < float(meta.get("min_profit_pct", 0.003)):
        await log.ainfo("chaser_potential_lost", symbol=symbol,
                        was=potential_at_signal, now=potential_now)
        return

    # Qty hesap
    balance = await get_total_wallet_balance()
    available = await get_available_balance()
    info = await get_exchange_info_cached(symbol)
    step_size = info["lotSize"]["stepSize"]
    min_qty = float(info["lotSize"]["minQty"])
    tick_size = info["priceFilter"]["tickSize"]
    min_notional = float(info.get("minNotional", {}).get("notional", 5))
    sym_cfg = await get_settings_or_defaults(symbol)
    sym_weight = sym_cfg.get("weight", 0.10)

    target = balance * sym_weight * 0.98
    usable = min(target, available * 0.95)
    raw_qty = usable / mark_now
    qty = round_step_size(raw_qty, step_size)
    if qty < min_qty or float(qty) * mark_now < min_notional:
        await log.awarning("chaser_qty_too_low", symbol=symbol,
                           qty=qty, min_qty=min_qty,
                           notional=float(qty) * mark_now, min_notional=min_notional)
        return

    # ── Market order (reduce_only=False, YENI poz) ──
    await log.awarning("chaser_market_entry_start", symbol=symbol,
                       side=order_side, qty=qty, mark=mark_now,
                       pine_tp=pine_tp, pine_sl=pine_sl,
                       potential_pct=round(potential_now * 100, 4))
    try:
        result = await place_market_order(symbol, order_side, float(qty), reduce_only=False)
        fill_price = float(result.get("avgPrice", 0)) or mark_now
    except Exception as e:
        await log.aerror("chaser_market_entry_failed", symbol=symbol, error=str(e))
        return

    # ── Verify (3 retry exponential backoff) ──
    verified = False
    verify_pos_amt = 0.0
    backoff = _settings.pine_chaser_verify_backoff_sec
    for attempt in range(_settings.pine_chaser_verify_retries):
        await asyncio.sleep(backoff)
        positions = await get_position_risk(symbol)
        for p in positions:
            if p.get("symbol") == symbol:
                verify_pos_amt = float(p.get("positionAmt", 0))
                break
        expected_sign = 1 if is_long else -1
        expected_qty = qty * expected_sign
        if abs(verify_pos_amt - expected_qty) < min_qty:
            verified = True
            break
        backoff *= 4  # exponential

    if not verified:
        await log.aerror("chaser_verify_failed", symbol=symbol,
                         expected=qty if is_long else -qty,
                         actual=verify_pos_amt,
                         retries=_settings.pine_chaser_verify_retries)
        # Kritik: poz gerçekten açılmamış olabilir ya da farklı qty. Manuel bakma!
        return

    await log.awarning("chaser_market_entry_verified", symbol=symbol,
                       side=order_side, qty=qty, fill_price=fill_price,
                       binance_pos_amt=verify_pos_amt)

    # ── TP + SL algo (algo_lock kullan) ──
    tp_rounded = float(round_price(pine_tp, tick_size))
    sl_rounded = float(round_price(pine_sl, tick_size))
    verify_qty = abs(verify_pos_amt)

    # TP
    tp_ok = False
    tp_lock = await tracker.try_acquire_sl_lock(symbol)
    if tp_lock:
        try:
            await place_take_profit_market_order(symbol, close_side, verify_qty, tp_rounded)
            tp_ok = True
        except Exception as e:
            await log.aerror("chaser_tp_place_failed", symbol=symbol, error=str(e))
        finally:
            await tracker.release_sl_lock(symbol)

    # SL
    sl_ok = False
    sl_lock = await tracker.try_acquire_sl_lock(symbol)
    if sl_lock:
        try:
            await place_stop_market_instant(symbol, close_side, verify_qty, sl_rounded)
            await tracker.mark_sl_placed(symbol)
            sl_ok = True
        except Exception as e:
            await log.aerror("chaser_sl_place_failed", symbol=symbol, error=str(e))
        finally:
            await tracker.release_sl_lock(symbol)

    await log.awarning("chaser_entry_complete", symbol=symbol,
                       side=order_side, qty=verify_qty, fill_price=fill_price,
                       tp=tp_rounded, sl=sl_rounded,
                       tp_ok=tp_ok, sl_ok=sl_ok)

    # Chaser is bitti — sil
    await disarm(symbol, reason="market_entry_success")


async def restore_all() -> None:
    """Startup: Redis'teki aktif chaser'lari yeniden başlat."""
    try:
        symbols = await tracker.list_chasers()
        for sym in symbols:
            _spawn_task(sym)
            await log.ainfo("chaser_restored", symbol=sym)
        if symbols:
            await log.ainfo("chaser_restore_count", n=len(symbols))
    except Exception as e:
        await log.awarning("chaser_restore_failed", error=str(e))


async def stop_all() -> None:
    """Shutdown — tum task'lari iptal et."""
    for sym, task in list(_tasks.items()):
        if not task.done():
            task.cancel()
    _tasks.clear()
