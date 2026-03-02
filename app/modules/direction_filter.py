"""Direction change filter – suppress consecutive same-direction signals."""

from __future__ import annotations

_DIR_TTL = 86400  # 24 hours


async def is_same_direction(r, symbol: str, indicator: str, tf: str, signal) -> bool:
    """Return True if *signal* is the same direction as the last stored signal.

    First signal for a given (symbol, indicator, tf) combination is always
    accepted (returns False).  When the direction changes the key is updated
    and the signal is accepted.
    """
    key = f"tv:signal_dir:{symbol}:{indicator}:{tf}"
    direction = signal.value if hasattr(signal, "value") else str(signal)

    last = await r.get(key)

    if last is None:
        # First signal – store and accept
        await r.set(key, direction, ex=_DIR_TTL)
        return False

    if last == direction:
        # Same direction – reject
        return True

    # Direction changed – update and accept
    await r.set(key, direction, ex=_DIR_TTL)
    return False
