"""RSI(10) Wilder's RMA hesaplama — TradingView uyumlu.

Bagimsiz modül. candle_store ve hidden_divergence tarafindan kullanilir.
"""

from __future__ import annotations


def calculate_rsi(closes: list[float], length: int = 10) -> list[float | None]:
    """Wilder's RMA ile RSI hesapla.

    Returns: closes ile ayni uzunlukta liste.
    Ilk `length` eleman None (yetersiz veri).
    """
    n = len(closes)
    result: list[float | None] = [None] * n

    if n < length + 1:
        return result

    # Delta, gain, loss
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        delta = closes[i] - closes[i - 1]
        gains[i] = max(delta, 0.0)
        losses[i] = max(-delta, 0.0)

    # Ilk avg: SMA (index 1..length dahil)
    avg_gain = sum(gains[1 : length + 1]) / length
    avg_loss = sum(losses[1 : length + 1]) / length

    if avg_loss == 0:
        result[length] = 100.0
    else:
        rs = avg_gain / avg_loss
        result[length] = round(100.0 - (100.0 / (1.0 + rs)), 4)

    # Sonraki degerler: RMA (Wilder's smoothing)
    for i in range(length + 1, n):
        avg_gain = (avg_gain * (length - 1) + gains[i]) / length
        avg_loss = (avg_loss * (length - 1) + losses[i]) / length

        if avg_loss == 0:
            result[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i] = round(100.0 - (100.0 / (1.0 + rs)), 4)

    return result


def calculate_rsi_with_state(closes: list[float], length: int = 10) -> tuple[list[float | None], dict]:
    """RSI hesapla + son mumun RMA state'ini dondur.

    Returns:
        (rsi_values, rsi_state)
        rsi_state: {"avg_gain": float, "avg_loss": float, "prev_close": float}
        Bu state ile canli mum RSI'i frontend'de hesaplanabilir.
    """
    n = len(closes)
    result: list[float | None] = [None] * n
    state = {"avg_gain": 0.0, "avg_loss": 0.0, "prev_close": 0.0}

    if n < length + 1:
        return result, state

    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        delta = closes[i] - closes[i - 1]
        gains[i] = max(delta, 0.0)
        losses[i] = max(-delta, 0.0)

    avg_gain = sum(gains[1 : length + 1]) / length
    avg_loss = sum(losses[1 : length + 1]) / length

    if avg_loss == 0:
        result[length] = 100.0
    else:
        rs = avg_gain / avg_loss
        result[length] = round(100.0 - (100.0 / (1.0 + rs)), 4)

    for i in range(length + 1, n):
        avg_gain = (avg_gain * (length - 1) + gains[i]) / length
        avg_loss = (avg_loss * (length - 1) + losses[i]) / length

        if avg_loss == 0:
            result[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i] = round(100.0 - (100.0 / (1.0 + rs)), 4)

    state = {
        "avg_gain": round(avg_gain, 10),
        "avg_loss": round(avg_loss, 10),
        "prev_close": closes[-1] if closes else 0.0,
    }

    return result, state
