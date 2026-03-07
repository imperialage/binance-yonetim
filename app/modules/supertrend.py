"""AlgoAlpha Adaptive Supertrend – Pine Script'ten birebir Python çevirisi.

Kaynak: AlgoAlpha "Adaptive Supertrend" Pine Script göstergesi.
K-Means clustering ile volatilite kümeleri belirler, ATR'yi uyarlar.
"""

from __future__ import annotations

from typing import Any


def _kmeans_on_window(
    window: list[float],
    highvol: float,
    midvol: float,
    lowvol: float,
) -> tuple[float, float, float]:
    """Run K-Means on ATR window, return (c_high, c_mid, c_low) centroids."""
    mn = min(window)
    mx = max(window)
    rng = mx - mn

    c_high = mn + rng * highvol
    c_mid = mn + rng * midvol
    c_low = mn + rng * lowvol

    for _ in range(20):
        s_high = 0.0; n_high = 0
        s_mid = 0.0; n_mid = 0
        s_low = 0.0; n_low = 0

        for v in window:
            d_high = abs(v - c_high)
            d_mid = abs(v - c_mid)
            d_low = abs(v - c_low)
            m = min(d_high, d_mid, d_low)

            if m == d_high:
                s_high += v; n_high += 1
            elif m == d_mid:
                s_mid += v; n_mid += 1
            else:
                s_low += v; n_low += 1

        new_high = s_high / n_high if n_high > 0 else c_high
        new_mid = s_mid / n_mid if n_mid > 0 else c_mid
        new_low = s_low / n_low if n_low > 0 else c_low

        if (abs(new_high - c_high) < 1e-10 and
            abs(new_mid - c_mid) < 1e-10 and
            abs(new_low - c_low) < 1e-10):
            return new_high, new_mid, new_low

        c_high, c_mid, c_low = new_high, new_mid, new_low

    return c_high, c_mid, c_low


def calculate_adaptive_supertrend(
    klines: list[list[Any]],
    atr_len: int = 10,
    factor: float = 3.0,
    training_period: int = 100,
    highvol: float = 0.75,
    midvol: float = 0.5,
    lowvol: float = 0.25,
) -> tuple[list[dict], list[dict]]:
    """Adaptive Supertrend hesapla.

    Returns:
        (candles, supertrend_data) tuple:
        - candles: [{time, open, high, low, close, volume}, ...]
        - supertrend_data: [{time, value, direction, cluster}, ...]
    """
    n = len(klines)
    if n < 2:
        return [], []

    # ── Parse klines ──
    times = [0] * n
    opens = [0.0] * n
    highs = [0.0] * n
    lows = [0.0] * n
    closes = [0.0] * n
    volumes = [0.0] * n

    for i, k in enumerate(klines):
        times[i] = int(k[0]) // 1000  # ms → unix seconds
        opens[i] = float(k[1])
        highs[i] = float(k[2])
        lows[i] = float(k[3])
        closes[i] = float(k[4])
        volumes[i] = float(k[5])

    # ── 1. True Range & ATR (Wilder's smoothing) ──
    tr = [0.0] * n
    atr = [0.0] * n

    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i - 1])
        lc = abs(lows[i] - closes[i - 1])
        tr[i] = max(hl, hc, lc)

    # Wilder's smoothing: first ATR = SMA, then RMA
    if n >= atr_len:
        atr[atr_len - 1] = sum(tr[:atr_len]) / atr_len
        for i in range(atr_len, n):
            atr[i] = (atr[i - 1] * (atr_len - 1) + tr[i]) / atr_len
        for i in range(atr_len - 1):
            atr[i] = atr[atr_len - 1]
    else:
        avg = sum(tr) / n if n > 0 else 0
        for i in range(n):
            atr[i] = avg

    # ── 2-4. K-Means clustering on ATR + assign centroid + cluster ──
    assigned_centroid = [0.0] * n
    cluster = [1] * n  # 0=high, 1=mid, 2=low

    for i in range(n):
        lookback = min(i + 1, training_period)
        window = atr[i - lookback + 1 : i + 1]

        if lookback < 2:
            assigned_centroid[i] = atr[i]
            cluster[i] = 1
            continue

        mn = min(window)
        rng = max(window) - mn
        if rng < 1e-12:
            assigned_centroid[i] = atr[i]
            cluster[i] = 1
            continue

        c_high, c_mid, c_low = _kmeans_on_window(window, highvol, midvol, lowvol)

        # Assign current ATR to nearest centroid
        cur = atr[i]
        d_h = abs(cur - c_high)
        d_m = abs(cur - c_mid)
        d_l = abs(cur - c_low)
        m = min(d_h, d_m, d_l)

        if m == d_h:
            assigned_centroid[i] = c_high
            cluster[i] = 0  # high volatility
        elif m == d_m:
            assigned_centroid[i] = c_mid
            cluster[i] = 1  # medium
        else:
            assigned_centroid[i] = c_low
            cluster[i] = 2  # low

    # ── 5-7. Supertrend calculation ──
    upper_band = [0.0] * n
    lower_band = [0.0] * n
    direction = [1] * n  # +1 bearish, -1 bullish (Pine convention)

    for i in range(n):
        hl2 = (highs[i] + lows[i]) / 2.0
        upper_band[i] = hl2 + factor * assigned_centroid[i]
        lower_band[i] = hl2 - factor * assigned_centroid[i]

    for i in range(1, n):
        # Band smoothing (standard supertrend logic)
        if not (lower_band[i] > lower_band[i - 1] or closes[i - 1] < lower_band[i - 1]):
            lower_band[i] = lower_band[i - 1]

        if not (upper_band[i] < upper_band[i - 1] or closes[i - 1] > upper_band[i - 1]):
            upper_band[i] = upper_band[i - 1]

    # Direction: first pass sets direction[0]=1 (bearish default)
    supertrend = [0.0] * n
    supertrend[0] = upper_band[0]  # initial = upper (bearish)

    for i in range(1, n):
        prev_st = supertrend[i - 1]
        if prev_st == upper_band[i - 1]:
            direction[i] = -1 if closes[i] > upper_band[i] else 1
        else:
            direction[i] = 1 if closes[i] < lower_band[i] else -1
        supertrend[i] = upper_band[i] if direction[i] == 1 else lower_band[i]

    # ── Build output ──
    candles: list[dict] = []
    st_data: list[dict] = []

    for i in range(n):
        candles.append({
            "time": times[i],
            "open": opens[i],
            "high": highs[i],
            "low": lows[i],
            "close": closes[i],
            "volume": volumes[i],
        })
        st_data.append({
            "time": times[i],
            "value": round(supertrend[i], 6),
            "direction": direction[i],
            "cluster": cluster[i],
        })

    return candles, st_data
