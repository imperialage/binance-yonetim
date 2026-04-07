"""Trailing Take-Profit Engine — dinamik kar koruma motoru.

TP mesafesinin yuzdesi uzerinden calisir:
- Kar belirli bir zirveye ulastiginda, geri cekilme limiti daralir
- Kar geri cekilme limitini asarsa pozisyon market ile kapatilir

Kural formati: "peak1:close1,peak2:close2,..."
Ornek: "40:10,70:40,90:70,99:90"
  → Kar TP'nin %40'ina ulasti, %10'a duserse kapat
  → Kar TP'nin %70'ine ulasti, %40'a duserse kapat
  → Kar TP'nin %90'ina ulasti, %70'e duserse kapat
  → Kar TP'nin %99'una ulasti, %90'a duserse kapat
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from app.modules.price_stream import get_live_price
from app.utils.logging import get_logger

log = get_logger(__name__)

_task: asyncio.Task | None = None

# Sembol bazli peak takibi
_peaks: dict[str, float] = {}  # symbol → en yuksek kar% (TP mesafesinin yuzdesi)
_prev_prices: dict[str, float] = {}  # symbol → onceki tick fiyati (flash crash icin)


def parse_rules(rules_str: str) -> list[tuple[float, float]]:
    """Kural string'ini parse et. "40:10,70:40,90:70" → [(40,10),(70,40),(90,70)]"""
    if not rules_str or not rules_str.strip():
        return []
    rules = []
    for part in rules_str.split(","):
        part = part.strip()
        if ":" not in part:
            continue
        try:
            peak, close_at = part.split(":")
            rules.append((float(peak.strip()), float(close_at.strip())))
        except (ValueError, TypeError):
            continue
    # Kucukten buyuge sirala
    rules.sort(key=lambda x: x[0])
    return rules


def calc_tp_progress(price: float, entry_price: float, tp_price: float, side: str) -> float:
    """Anlık fiyattan TP mesafesinin yuzde kacinda oldugumuzu hesapla.

    Returns: 0-100+ arasi float. 100 = TP'ye ulasti.
    """
    if side == "LONG":
        tp_distance = tp_price - entry_price
        current_distance = price - entry_price
    else:
        tp_distance = entry_price - tp_price
        current_distance = entry_price - price

    if tp_distance <= 0:
        return 0.0
    return (current_distance / tp_distance) * 100


async def _trailing_loop() -> None:
    """Ana dongu — her 200ms tum pozisyonlari kontrol et."""
    from app.modules.signal_engine import get_all_engines
    from app.modules.ha_signal_engine import get_all_ha_engines
    from app.modules.indicator_settings_store import get_settings_or_defaults
    from app.modules.binance_client import place_market_order, cancel_all_open_orders

    await log.ainfo("trailing_tp_started")

    try:
        while True:
            await asyncio.sleep(0.2)

            # Tum engine'leri topla (normal + HA)
            all_engines: dict[str, Any] = {}
            try:
                for sym, eng in get_all_engines().items():
                    all_engines[sym] = eng
                for sym, eng in get_all_ha_engines().items():
                    all_engines[sym] = eng
            except Exception:
                continue

            for sym, engine in all_engines.items():
                if not engine.has_position or not engine.entry_price or engine.entry_price <= 0:
                    # Pozisyon yok — peak sifirla
                    if sym in _peaks:
                        del _peaks[sym]
                    continue

                # Ayarlari al
                try:
                    cfg = await get_settings_or_defaults(sym)
                except Exception:
                    continue

                if not cfg.get("trailing_tp_enabled"):
                    continue

                rules_str = cfg.get("trailing_tp_rules", "")
                rules = parse_rules(rules_str)
                if not rules:
                    continue

                # Fiyat al
                price = get_live_price(sym)
                if price is None:
                    continue

                # TP fiyatini hesapla
                tp_pct = cfg.get("tp_pct", 1.0) / 100.0
                entry = engine.entry_price
                side = engine.position_side

                if side == "LONG":
                    tp_price = entry * (1 + tp_pct)
                elif side == "SHORT":
                    tp_price = entry * (1 - tp_pct)
                else:
                    continue

                # TP mesafesinin yuzdesi
                progress = calc_tp_progress(price, entry, tp_price, side)

                # ── Flash crash korumasi ──
                # Tek tick'te buyuk dusus → hemen kapat (SL'yi bekleme)
                prev_price = _prev_prices.get(sym)
                _prev_prices[sym] = price
                if prev_price and prev_price > 0:
                    if side == "LONG":
                        tick_drop_pct = (prev_price - price) / prev_price * 100
                    else:
                        tick_drop_pct = (price - prev_price) / prev_price * 100
                    # Tek tick'te %0.3+ dusus = flash crash → hemen kapat
                    flash_thresh = 0.3
                    if tick_drop_pct >= flash_thresh and progress < 50:
                        await log.ainfo("trailing_tp_flash_crash", symbol=sym, side=side,
                                        drop_pct=round(tick_drop_pct, 3), progress=round(progress, 2))
                        try:
                            await cancel_all_open_orders(sym)
                            close_side = "SELL" if side == "LONG" else "BUY"
                            qty = engine.entry_qty
                            if qty <= 0:
                                from app.modules.binance_client import get_position_risk
                                positions = await get_position_risk(sym)
                                for p in positions:
                                    if p.get("symbol") == sym:
                                        qty = abs(float(p.get("positionAmt", 0)))
                                        break
                            if qty > 0:
                                await place_market_order(sym, close_side, qty, reduce_only=True)
                                await log.ainfo("trailing_tp_flash_closed", symbol=sym, qty=qty)
                            _peaks.pop(sym, None)
                        except Exception as e:
                            await log.aerror("trailing_tp_flash_error", symbol=sym, error=str(e))
                        continue

                # Peak guncelle
                prev_peak = _peaks.get(sym, 0.0)
                if progress > prev_peak:
                    _peaks[sym] = progress

                current_peak = _peaks.get(sym, 0.0)

                # Kural kontrol — en yuksek eslesen kurali bul
                active_rule = None
                for (peak_thresh, close_at) in rules:
                    if current_peak >= peak_thresh:
                        active_rule = (peak_thresh, close_at)

                if active_rule is None:
                    continue  # henuz hicbir kurala ulasilmadi

                peak_thresh, close_at = active_rule

                # Kar geri cekildiyse kapat
                if progress <= close_at:
                    await log.ainfo(
                        "trailing_tp_triggered",
                        symbol=sym,
                        side=side,
                        entry=entry,
                        price=price,
                        peak_pct=round(current_peak, 2),
                        current_pct=round(progress, 2),
                        rule=f"{peak_thresh}:{close_at}",
                    )

                    # Market close
                    try:
                        await cancel_all_open_orders(sym)
                        close_side = "SELL" if side == "LONG" else "BUY"
                        qty = engine.entry_qty if engine.entry_qty > 0 else abs(float(getattr(engine, 'entry_qty', 0)))

                        # Qty yoksa Binance'tan al
                        if qty <= 0:
                            try:
                                from app.modules.binance_client import get_position_risk
                                positions = await get_position_risk(sym)
                                for p in positions:
                                    if p.get("symbol") == sym:
                                        qty = abs(float(p.get("positionAmt", 0)))
                                        break
                            except Exception:
                                pass

                        if qty > 0:
                            await place_market_order(sym, close_side, qty, reduce_only=True)
                            await log.ainfo("trailing_tp_closed", symbol=sym, side=close_side, qty=qty,
                                            peak=round(current_peak, 2), closed_at=round(progress, 2))
                        else:
                            await log.awarning("trailing_tp_no_qty", symbol=sym)

                    except Exception as e:
                        await log.aerror("trailing_tp_close_error", symbol=sym, error=str(e))

                    # Peak sifirla
                    _peaks.pop(sym, None)

    except asyncio.CancelledError:
        await log.ainfo("trailing_tp_stopped")


def start_trailing_tp() -> asyncio.Task:
    global _task
    if _task is not None and not _task.done():
        return _task
    _task = asyncio.create_task(_trailing_loop())
    return _task


async def stop_trailing_tp() -> None:
    global _task
    if _task is not None and not _task.done():
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        _task = None


def get_trailing_status() -> dict:
    """Debug icin mevcut peak durumu."""
    return {"peaks": {k: round(v, 2) for k, v in _peaks.items()}}
