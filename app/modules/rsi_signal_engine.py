"""RSI Momentum signal engine — XAGUSDT icin ozel.

Kural: Son 2 mumda RSI degisimi >= 20 ise BUY, <= -20 ise SELL.
SuperTrend'den bagimsiz calisir.
"""

from __future__ import annotations

from app.modules.candle_store import get_recent_rsi
from app.utils.logging import get_logger

log = get_logger(__name__)


async def check_rsi_momentum(
    symbol: str,
    interval: str = "15m",
    threshold: float = 20.0,
) -> dict | None:
    """Son 3 mumun RSI degerlerini kontrol et.

    rsi_change_2 = RSI[now] - RSI[2_bars_ago]
    >= +threshold → BUY
    <= -threshold → SELL
    else → None

    Returns:
        {"direction": "BUY"|"SELL", "rsi_now": float, "rsi_2ago": float, "rsi_change": float}
        or None if no signal.
    """
    recent = await get_recent_rsi(symbol, interval, count=3)

    if len(recent) < 3:
        return None

    rsi_now = recent[-1].get("rsi_10")
    rsi_2ago = recent[-3].get("rsi_10")

    if rsi_now is None or rsi_2ago is None:
        return None

    rsi_change = rsi_now - rsi_2ago

    if rsi_change >= threshold:
        await log.ainfo(
            "rsi_momentum_signal",
            symbol=symbol,
            direction="BUY",
            rsi_now=rsi_now,
            rsi_2ago=rsi_2ago,
            rsi_change=round(rsi_change, 2),
        )
        return {
            "direction": "BUY",
            "rsi_now": rsi_now,
            "rsi_2ago": rsi_2ago,
            "rsi_change": round(rsi_change, 2),
            "close": recent[-1].get("close"),
            "date": recent[-1].get("date"),
        }

    if rsi_change <= -threshold:
        await log.ainfo(
            "rsi_momentum_signal",
            symbol=symbol,
            direction="SELL",
            rsi_now=rsi_now,
            rsi_2ago=rsi_2ago,
            rsi_change=round(rsi_change, 2),
        )
        return {
            "direction": "SELL",
            "rsi_now": rsi_now,
            "rsi_2ago": rsi_2ago,
            "rsi_change": round(rsi_change, 2),
            "close": recent[-1].get("close"),
            "date": recent[-1].get("date"),
        }

    return None
