"""GET /events – browse normalised events for a symbol."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from app.modules.redis_client import get_redis

router = APIRouter()


_TZ_TR = timezone(timedelta(hours=3))


def _enrich_event(ev: dict[str, Any]) -> dict[str, Any]:
    """Add human-readable datetime fields to an event dict (Turkey UTC+3)."""
    ts = ev.get("ts")
    recv = ev.get("received_at")
    if ts:
        ev["ts_human"] = datetime.fromtimestamp(ts, tz=_TZ_TR).strftime("%Y-%m-%d %H:%M:%S TR")
    if recv:
        ev["received_at_human"] = datetime.fromtimestamp(recv, tz=_TZ_TR).strftime("%Y-%m-%d %H:%M:%S TR")
    return ev


@router.get("/events")
async def get_events(
    symbol: str = Query(..., examples=["ETHUSDT"]),
    limit: int = Query(50, ge=1, le=500),
    indicator: str | None = Query(None),
    tf: str | None = Query(None),
    signal: str | None = Query(None),
    after: str | None = Query(None, description="Filter: only events after this datetime (YYYY-MM-DD or YYYY-MM-DD HH:MM)"),
    before: str | None = Query(None, description="Filter: only events before this datetime (YYYY-MM-DD or YYYY-MM-DD HH:MM)"),
) -> JSONResponse:
    r = await get_redis()
    symbol = symbol.strip().upper()

    # Parse date filters
    after_ts: int | None = None
    before_ts: int | None = None
    for label, raw_val, target in [("after", after, "after_ts"), ("before", before, "before_ts")]:
        if raw_val:
            parsed = _parse_datetime(raw_val)
            if parsed is None:
                return JSONResponse(
                    status_code=400,
                    content={"detail": f"Invalid {label} format: '{raw_val}'. Use YYYY-MM-DD or YYYY-MM-DD HH:MM"},
                )
            if target == "after_ts":
                after_ts = parsed
            else:
                before_ts = parsed

    raw_list = await r.lrange(f"tv:events:{symbol}", -limit * 3, -1)
    events: list[dict[str, Any]] = []
    for raw in reversed(raw_list):  # newest first
        try:
            ev = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue

        ev.pop("secret", None)
        if ev.get("raw"):
            ev["raw"].pop("secret", None)

        # Time filters
        ev_ts = ev.get("ts", 0)
        if after_ts and ev_ts < after_ts:
            continue
        if before_ts and ev_ts > before_ts:
            continue

        # Field filters
        if indicator and ev.get("indicator", "").lower() != indicator.lower():
            continue
        if tf and ev.get("tf", "").lower() != tf.lower():
            continue
        if signal and ev.get("signal", "").upper() != signal.upper():
            continue

        events.append(_enrich_event(ev))
        if len(events) >= limit:
            break

    return JSONResponse(content={"symbol": symbol, "count": len(events), "events": events})


def _parse_datetime(val: str) -> int | None:
    """Parse a date/datetime string to unix timestamp. Returns None on failure."""
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(val.strip(), fmt)
            return int(dt.timestamp())
        except ValueError:
            continue
    return None
