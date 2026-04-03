"""GET /status – health check endpoint."""

from __future__ import annotations

import asyncio
import time

import httpx
from fastapi import APIRouter
from pydantic import BaseModel

from app.modules.redis_client import get_redis, redis_ping
from app.utils.logging import get_logger

log = get_logger(__name__)
router = APIRouter()


class StatusResponse(BaseModel):
    status: str
    redis_ok: bool
    events_last_minute: int
    uptime_seconds: int


_start_time = time.time()


@router.get("/status", response_model=StatusResponse)
async def status() -> StatusResponse:
    redis_ok = await redis_ping()

    # Count events in the last minute across known rate buckets
    events_count = 0
    try:
        r = await get_redis()
        bucket = int(time.time()) // 10
        # Scan recent rate keys (last 6 buckets = ~60 seconds)
        for offset in range(6):
            pattern = f"tv:rate:*:{bucket - offset}"
            async for key in r.scan_iter(match=pattern, count=100):
                val = await r.get(key)
                if val:
                    events_count += int(val)
    except Exception:
        pass

    return StatusResponse(
        status="ok" if redis_ok else "degraded",
        redis_ok=redis_ok,
        events_last_minute=events_count,
        uptime_seconds=int(time.time() - _start_time),
    )


@router.get("/server-ip")
async def server_ip() -> dict:
    """Return the server's outbound public IP address."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get("https://api.ipify.org")
        return {"ip": resp.text.strip()}


@router.get("/debug/proxy")
async def debug_proxy() -> dict:
    """Test Binance API connection through proxy."""
    from app.config import settings
    result: dict = {
        "proxy_configured": bool(settings.binance_proxy_url),
        "proxy_url": settings.binance_proxy_url[:30] + "..." if settings.binance_proxy_url else "",
        "trading_enabled": settings.trading_enabled,
    }
    import traceback
    try:
        from app.modules.binance_client import close_client, get_client
        await close_client()  # Force fresh client
        client = await get_client()
        resp = await client.get("/fapi/v1/premiumIndex", params={"symbol": "ETHUSDT"})
        resp.raise_for_status()
        data = resp.json()
        result["binance_ok"] = True
        result["mark_price"] = data.get("markPrice")
    except Exception as e:
        result["binance_ok"] = False
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
    return result


@router.get("/debug/trade-test")
async def debug_trade_test() -> dict:
    """Test a full signed Binance API call (balance check)."""
    import traceback
    result: dict = {}
    try:
        from app.modules.binance_client import get_usdt_balance, get_position_risk
        balance = await get_usdt_balance()
        result["balance"] = balance
        positions = await get_position_risk("ETHUSDT")
        for p in positions:
            if p.get("symbol") == "ETHUSDT":
                result["position_amt"] = p.get("positionAmt")
                break
        result["ok"] = True
    except Exception as e:
        result["ok"] = False
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
    return result


@router.get("/debug/diagnosis")
async def debug_diagnosis() -> dict:
    """Full system diagnosis: config, direction filters, recent trades, positions."""
    from app.config import settings
    from app.modules.redis_client import get_redis
    from app.modules.trade_store import query_trades

    result: dict = {
        "config": {
            "trading_enabled": settings.trading_enabled,
            "binance_testnet": settings.binance_testnet,
            "stop_loss_pct": settings.stop_loss_pct,
            "take_profit_pct": settings.take_profit_pct,
            "proxy_configured": bool(settings.binance_proxy_url),
            "trading_symbols": settings.trading_symbols,
            "trading_timeframes": settings.trading_timeframes,
            "strategies": {
                "1m": {"sl_pct": settings.strategy_1m_sl_pct, "tp_pct": settings.strategy_1m_tp_pct},
                "5m": {"sl_pct": settings.strategy_5m_sl_pct, "tp_pct": settings.strategy_5m_tp_pct},
            },
        },
    }

    # Direction filter state from Redis
    try:
        r = await get_redis()
        direction_keys: list[dict] = []
        async for key in r.scan_iter(match="tv:signal_dir:*", count=200):
            val = await r.get(key)
            ttl = await r.ttl(key)
            direction_keys.append({
                "key": key if isinstance(key, str) else key.decode(),
                "value": val if isinstance(val, str) else (val.decode() if val else None),
                "ttl_seconds": ttl,
            })
        result["direction_filters"] = direction_keys
    except Exception as e:
        result["direction_filters_error"] = str(e)

    # Recent trades from SQLite
    try:
        trades = await query_trades(limit=10)
        result["recent_trades"] = trades
    except Exception as e:
        result["recent_trades_error"] = str(e)

    # Current Binance positions — all trading symbols
    try:
        from app.config import SYMBOL_CONFIGS, get_symbol_config
        from app.modules.binance_client import get_usdt_balance, get_position_risk
        balance = await get_usdt_balance()
        result["balance"] = balance

        # Fetch positions sequentially to avoid Binance rate limit
        trading_syms = list(SYMBOL_CONFIGS.keys())
        pos_results = []
        for s in trading_syms:
            try:
                r = await get_position_risk(s)
                pos_results.append(r)
            except Exception as e:
                pos_results.append(e)

        positions_map: dict = {}
        for sym, pos_list in zip(trading_syms, pos_results):
            if isinstance(pos_list, Exception):
                continue
            for p in pos_list:
                if p.get("symbol") == sym:
                    sym_cfg = get_symbol_config(sym)
                    positions_map[sym] = {
                        "positionAmt": p.get("positionAmt"),
                        "entryPrice": p.get("entryPrice"),
                        "unRealizedProfit": p.get("unRealizedProfit"),
                        "markPrice": p.get("markPrice"),
                        "updateTime": p.get("updateTime"),
                        "tp_pct": sym_cfg.get("tp_pct", 0.005),
                        "sl_pct": sym_cfg.get("sl_pct", 0.015),
                        "weight": sym_cfg.get("weight", 0.10),
                    }
                    break

        result["positions"] = positions_map
        # Backward compat: keep single "position" for ETHUSDT
        if "ETHUSDT" in positions_map:
            result["position"] = positions_map["ETHUSDT"]
    except Exception as e:
        result["binance_error"] = str(e)

    return result


@router.get("/debug/income")
async def debug_income() -> dict:
    """Get realized PnL history with entry/exit prices (last 7 days), all trading symbols."""
    from app.config import SYMBOL_CONFIGS
    from app.modules.binance_client import get_income_history, get_user_trades
    from datetime import datetime, timezone, timedelta
    tz = timezone(timedelta(hours=3))

    all_symbols = list(SYMBOL_CONFIGS.keys()) or ["ETHUSDT"]
    all_trades: list[dict] = []
    grand_total_pnl = 0.0

    for sym in all_symbols:
      try:
        records, user_trades = await asyncio.gather(
            get_income_history(sym, "REALIZED_PNL", days=7),
            get_user_trades(sym, days=7),
        )

        # ── 1. Split each userTrade fill into OPEN or CLOSE portion ──
        # A single fill can be a closer (rpnl ≠ 0) or opener (rpnl ≈ 0).
        # Reversal orders have BOTH types under the same orderId.
        open_fills: list[dict] = []   # position openers
        close_fills: list[dict] = []  # position closers

        for ut in user_trades:
            fill = {
                "order_id": str(ut.get("orderId", "")),
                "trade_id": str(ut.get("id", "")),
                "price": float(ut.get("price", 0)),
                "qty": float(ut.get("qty", 0)),
                "rpnl": float(ut.get("realizedPnl", 0)),
                "side": ut.get("side", ""),
                "ts_ms": int(ut.get("time", 0)),
            }
            if abs(fill["rpnl"]) > 0.0001:
                close_fills.append(fill)
            else:
                open_fills.append(fill)

        # ── 2. Group CLOSE fills by orderId ──
        close_orders: dict[str, dict] = {}
        for f in close_fills:
            oid = f["order_id"]
            if oid not in close_orders:
                close_orders[oid] = {
                    "order_id": oid, "side": f["side"],
                    "total_qty": 0.0, "total_value": 0.0,
                    "rpnl": 0.0, "ts_ms": f["ts_ms"], "trade_ids": [],
                }
            o = close_orders[oid]
            o["total_qty"] += f["qty"]
            o["total_value"] += f["price"] * f["qty"]
            o["rpnl"] += f["rpnl"]
            o["trade_ids"].append(f["trade_id"])
            if f["ts_ms"] > o["ts_ms"]:
                o["ts_ms"] = f["ts_ms"]

        # ── 3. Group OPEN fills by orderId ──
        open_orders: dict[str, dict] = {}
        for f in open_fills:
            oid = f["order_id"]
            if oid not in open_orders:
                open_orders[oid] = {
                    "order_id": oid, "side": f["side"],
                    "total_qty": 0.0, "total_value": 0.0,
                    "ts_ms": f["ts_ms"], "trade_ids": [],
                }
            o = open_orders[oid]
            o["total_qty"] += f["qty"]
            o["total_value"] += f["price"] * f["qty"]
            o["trade_ids"].append(f["trade_id"])
            if f["ts_ms"] > o["ts_ms"]:
                o["ts_ms"] = f["ts_ms"]

        # Calculate avg price
        for o in close_orders.values():
            o["avg_price"] = round(o["total_value"] / o["total_qty"], 2) if o["total_qty"] > 0 else 0
        for o in open_orders.values():
            o["avg_price"] = round(o["total_value"] / o["total_qty"], 2) if o["total_qty"] > 0 else 0

        # ── 4. Match each close order with its nearest preceding open ──
        open_list = sorted(open_orders.values(), key=lambda x: x["ts_ms"])
        close_list = sorted(close_orders.values(), key=lambda x: x["ts_ms"])

        used_opens: set[str] = set()
        close_to_open: dict[str, dict] = {}
        for co in close_list:
            close_side = co["side"]
            open_side = "SELL" if close_side == "BUY" else "BUY"
            best_open = None
            for oo in reversed(open_list):
                if oo["order_id"] in used_opens:
                    continue
                if oo["side"] == open_side and oo["ts_ms"] <= co["ts_ms"]:
                    best_open = oo
                    break
            if best_open:
                used_opens.add(best_open["order_id"])
                close_to_open[co["order_id"]] = best_open

        # ── 5. Sum income by orderId for accurate PnL ──
        trade_to_order: dict[str, str] = {}
        for ut in user_trades:
            trade_to_order[str(ut.get("id", ""))] = str(ut.get("orderId", ""))

        income_by_order: dict[str, float] = {}
        income_time_by_order: dict[str, int] = {}
        total_pnl = 0.0
        for r in records:
            pnl = float(r.get("income", 0))
            if pnl == 0:
                continue
            total_pnl += pnl
            tid = str(r.get("info", ""))
            oid = trade_to_order.get(tid, tid)
            income_by_order[oid] = income_by_order.get(oid, 0) + pnl
            r_time = int(r.get("time", 0))
            if r_time > income_time_by_order.get(oid, 0):
                income_time_by_order[oid] = r_time

        # ── 6. Build trade list from close orders ──
        trades = []
        seen_income_oids: set[str] = set()

        for co in close_list:
            oid = co["order_id"]
            exit_price = co["avg_price"]
            exit_side = co["side"]
            qty = round(co["total_qty"], 4)
            pnl = income_by_order.get(oid, co["rpnl"])
            close_time = datetime.fromtimestamp(co["ts_ms"] / 1000, tz=tz).strftime("%Y-%m-%d %H:%M:%S")

            # Mark income as matched
            seen_income_oids.add(oid)
            for tid in co["trade_ids"]:
                seen_income_oids.add(tid)

            # Entry from matched open order
            open_order = close_to_open.get(oid)
            if open_order:
                entry_price = open_order["avg_price"]
                entry_time = datetime.fromtimestamp(open_order["ts_ms"] / 1000, tz=tz).strftime("%Y-%m-%d %H:%M:%S")
            else:
                entry_time = ""
                if exit_price and qty:
                    if exit_side == "BUY":
                        entry_price = round(exit_price + pnl / qty, 2)
                    else:
                        entry_price = round(exit_price - pnl / qty, 2)
                else:
                    entry_price = 0.0

            pos_side = "LONG" if exit_side == "SELL" else "SHORT" if exit_side == "BUY" else ""

            trades.append({
                "entry_time": entry_time,
                "time": close_time,
                "side": pos_side,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "qty": qty,
                "pnl": round(pnl, 6),
                "symbol": sym,
            })

        # ── 7. Fallback: income records with no matching userTrade ──
        for oid, pnl in income_by_order.items():
            if oid in seen_income_oids:
                continue
            r_time = income_time_by_order.get(oid, 0)
            close_time = datetime.fromtimestamp(r_time / 1000, tz=tz).strftime("%Y-%m-%d %H:%M:%S") if r_time else ""
            trades.append({
                "entry_time": "",
                "time": close_time,
                "side": "",
                "entry_price": 0.0,
                "exit_price": 0.0,
                "qty": 0.0,
                "pnl": round(pnl, 6),
                "symbol": sym,
            })

        all_trades.extend(trades)
        grand_total_pnl += total_pnl
      except Exception:
        continue  # Skip symbol on error, continue with others

    # Sort all trades newest first
    all_trades.sort(key=lambda t: t["time"], reverse=True)
    win = sum(1 for t in all_trades if t["pnl"] > 0)
    lose = sum(1 for t in all_trades if t["pnl"] < 0)
    return {
        "total_pnl": round(grand_total_pnl, 6),
        "trade_count": len(all_trades),
        "win": win,
        "lose": lose,
        "win_rate": f"{(win / len(all_trades) * 100):.0f}%" if all_trades else "0%",
        "trades": all_trades,
    }


@router.get("/api/live-rsi")
async def api_live_rsi(symbol: str = "XAGUSDT", interval: str = "15m", rsi_len: int = 10) -> dict:
    """Canli RSI — 1sn polling icin. Hafif, hizli."""
    from app.modules.live_rsi import get_live_rsi
    from app.modules.price_stream import get_live_price

    price = get_live_price(symbol.upper())
    if price is None:
        return {"symbol": symbol.upper(), "rsi": None, "price": None, "error": "no_price"}

    rsi = await get_live_rsi(symbol.upper(), interval, price, rsi_len)
    return {"symbol": symbol.upper(), "interval": interval, "rsi": rsi, "price": price}


@router.get("/api/chart-data")
async def api_chart_data(
    symbol: str = "XAGUSDT",
    interval: str = "15m",
    rsi_len: int = 10,
    long_thresh: float = 32,
    short_thresh: float = 70,
    max_gap: int = 12,
    entry_buffer: float = 0.1,
    tp_pct_param: float = 0,
    sl_pct_param: float = 0,
    start_date: str = "",
    end_date: str = "",
) -> dict:
    """Grafik icin mum + RSI + sinyal (server-side hesaplama) + pozisyon verisi.

    Binance API'den direkt mum ceker — tum semboller ve TF'ler desteklenir.
    start_date/end_date: YYYY-MM-DD formatinda tarih filtresi.
    """
    from app.modules.rsi_calculator import calculate_rsi
    from app.modules.binance_client import get_position_risk
    from app.config import get_symbol_config
    from datetime import datetime, timezone, timedelta
    import httpx

    tz_ist = timezone(timedelta(hours=3))
    sym = symbol.upper()
    buf = entry_buffer / 100

    cfg = get_symbol_config(sym)
    tp_pct = tp_pct_param / 100 if tp_pct_param > 0 else cfg.get("tp_pct", 0.01)
    sl_pct = sl_pct_param / 100 if sl_pct_param > 0 else cfg.get("sl_pct", 0.003)

    # Binance API'den mum cek (tarih filtreli, paginated)
    import time as _time

    INTERVAL_MS = {"1m":60000,"5m":300000,"15m":900000,"30m":1800000,"1h":3600000,"4h":14400000,"1d":86400000}
    iv_ms = INTERVAL_MS.get(interval, 900000)

    # Tarih araligi hesapla
    if start_date:
        try:
            start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
            # RSI warmup icin 200 mum oncesinden basla
            warmup_ms = start_ms - (1000 * iv_ms)
        except Exception:
            warmup_ms = int(_time.time() * 1000) - (1500 * iv_ms)
            start_ms = warmup_ms
    else:
        warmup_ms = int(_time.time() * 1000) - (1500 * iv_ms)
        start_ms = warmup_ms

    end_ms = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000) + 86400000 if end_date else int(_time.time() * 1000)

    try:
        raw_klines = []
        current = warmup_ms
        async with httpx.AsyncClient(timeout=15) as client:
            while current < end_ms:
                resp = await client.get("https://fapi.binance.com/fapi/v1/klines", params={
                    "symbol": sym, "interval": interval, "startTime": current, "endTime": end_ms, "limit": 1500,
                })
                resp.raise_for_status()
                batch = resp.json()
                if not batch:
                    break
                raw_klines.extend(batch)
                current = int(batch[-1][0]) + iv_ms
                if len(batch) < 1500:
                    break
                import asyncio
                await asyncio.sleep(0.2)
    except Exception as e:
        return {"candles": [], "signals": [], "trades": [], "position": None, "error": str(e)}

    if not raw_klines:
        return {"candles": [], "signals": [], "trades": [], "position": None}

    # Parse + RSI
    closes = [float(k[4]) for k in raw_klines]
    rsi_values = calculate_rsi(closes, rsi_len)

    # RSI state hesapla — sondan bir onceki mumun state'i (canli mum icin)
    # Wilder's RMA'yi yeniden calistir, sondan bir onceki adimda dur
    rsi_state = None
    n = len(closes)
    if n >= rsi_len + 2:
        gains = [0.0] * n
        losses_arr = [0.0] * n
        for i in range(1, n):
            d = closes[i] - closes[i - 1]
            gains[i] = max(d, 0.0)
            losses_arr[i] = max(-d, 0.0)
        ag = sum(gains[1:rsi_len + 1]) / rsi_len
        al = sum(losses_arr[1:rsi_len + 1]) / rsi_len
        for i in range(rsi_len + 1, n - 1):  # n-1'de dur (sondan bir onceki)
            ag = (ag * (rsi_len - 1) + gains[i]) / rsi_len
            al = (al * (rsi_len - 1) + losses_arr[i]) / rsi_len
        rsi_state = {
            "avg_gain": round(ag, 12),
            "avg_loss": round(al, 12),
            "prev_close": closes[-2],  # sondan bir onceki mum
        }

    candles = []
    for i, k in enumerate(raw_klines):
        candles.append({
            "time": int(k[0]) // 1000,
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "rsi": rsi_values[i] if i < len(rsi_values) else None,
        })

    # Hidden Divergence sinyal tespiti (server-side)
    signals = []
    trades = []
    used_a = set()
    state = 0  # 0=flat, 1=long, -1=short
    entry = 0.0
    tp_price = 0.0
    sl_price = 0.0
    entry_idx = 0
    entry_time = 0

    n = len(candles)
    for b_idx in range(max_gap + 1, n):
        rsi_b = candles[b_idx].get("rsi")
        if rsi_b is None:
            continue

        # TP/SL kontrol (acik pozisyon)
        if state == 1 and b_idx > entry_idx:
            if candles[b_idx]["low"] <= sl_price:
                pnl = (sl_price - entry) / entry * 100 - 0.08
                trades.append({"entry_time": entry_time, "exit_time": candles[b_idx]["time"],
                    "entry_date": datetime.fromtimestamp(entry_time, tz=tz_ist).strftime("%d.%m %H:%M") if entry_time > 0 else "",
                    "exit_date": datetime.fromtimestamp(candles[b_idx]["time"], tz=tz_ist).strftime("%d.%m %H:%M"),
                    "direction": "BUY", "entry_price": entry, "exit_price": sl_price,
                    "reason": "SL", "pnl_pct": round(pnl, 3)})
                state = 0
            elif candles[b_idx]["high"] >= tp_price:
                pnl = (tp_price - entry) / entry * 100 - 0.08
                trades.append({"entry_time": entry_time, "exit_time": candles[b_idx]["time"],
                    "entry_date": datetime.fromtimestamp(entry_time, tz=tz_ist).strftime("%d.%m %H:%M") if entry_time > 0 else "",
                    "exit_date": datetime.fromtimestamp(candles[b_idx]["time"], tz=tz_ist).strftime("%d.%m %H:%M"),
                    "direction": "BUY", "entry_price": entry, "exit_price": tp_price,
                    "reason": "TP", "pnl_pct": round(pnl, 3)})
                state = 0

        elif state == -1 and b_idx > entry_idx:
            if candles[b_idx]["high"] >= sl_price:
                pnl = (entry - sl_price) / entry * 100 - 0.08
                trades.append({"entry_time": entry_time, "exit_time": candles[b_idx]["time"],
                    "entry_date": datetime.fromtimestamp(entry_time, tz=tz_ist).strftime("%d.%m %H:%M") if entry_time > 0 else "",
                    "exit_date": datetime.fromtimestamp(candles[b_idx]["time"], tz=tz_ist).strftime("%d.%m %H:%M"),
                    "direction": "SELL", "entry_price": entry, "exit_price": sl_price,
                    "reason": "SL", "pnl_pct": round(pnl, 3)})
                state = 0
            elif candles[b_idx]["low"] <= tp_price:
                pnl = (entry - tp_price) / entry * 100 - 0.08
                trades.append({"entry_time": entry_time, "exit_time": candles[b_idx]["time"],
                    "entry_date": datetime.fromtimestamp(entry_time, tz=tz_ist).strftime("%d.%m %H:%M") if entry_time > 0 else "",
                    "exit_date": datetime.fromtimestamp(candles[b_idx]["time"], tz=tz_ist).strftime("%d.%m %H:%M"),
                    "direction": "SELL", "entry_price": entry, "exit_price": tp_price,
                    "reason": "TP", "pnl_pct": round(pnl, 3)})
                state = 0

        # Sinyal tespiti (sadece flat iken)
        if state != 0:
            continue

        found = False
        # SHORT ara
        for gap in range(1, min(max_gap + 1, b_idx)):
            a_idx = b_idx - gap
            if a_idx in used_a:
                continue
            rsi_a = candles[a_idx].get("rsi")
            if rsi_a is None:
                continue
            if rsi_a >= short_thresh and candles[b_idx]["high"] > candles[a_idx]["high"] and rsi_b < rsi_a:
                ep = round(candles[b_idx]["high"] * (1 - buf), 6)
                sig = {"time": candles[b_idx]["time"], "direction": "SELL", "entry_price": ep,
                    "rsi_a": rsi_a, "rsi_b": rsi_b, "gap": gap,
                    "date": datetime.fromtimestamp(candles[b_idx]["time"], tz=tz_ist).strftime("%d.%m %H:%M")}
                signals.append(sig)
                used_a.add(a_idx)
                state = -1
                entry = ep
                tp_price = ep * (1 - tp_pct)
                sl_price = ep * (1 + sl_pct)
                entry_idx = b_idx
                entry_time = candles[b_idx]["time"]
                found = True
                break

        if found:
            continue

        # LONG ara
        for gap in range(1, min(max_gap + 1, b_idx)):
            a_idx = b_idx - gap
            if a_idx in used_a:
                continue
            rsi_a = candles[a_idx].get("rsi")
            if rsi_a is None:
                continue
            if rsi_a <= long_thresh and candles[b_idx]["low"] < candles[a_idx]["low"] and rsi_b > rsi_a:
                ep = round(candles[b_idx]["low"] * (1 + buf), 6)
                sig = {"time": candles[b_idx]["time"], "direction": "BUY", "entry_price": ep,
                    "rsi_a": rsi_a, "rsi_b": rsi_b, "gap": gap,
                    "date": datetime.fromtimestamp(candles[b_idx]["time"], tz=tz_ist).strftime("%d.%m %H:%M")}
                signals.append(sig)
                used_a.add(a_idx)
                state = 1
                entry = ep
                tp_price = ep * (1 + tp_pct)
                sl_price = ep * (1 - sl_pct)
                entry_idx = b_idx
                entry_time = candles[b_idx]["time"]
                break

    # Binance acik pozisyon
    position = None
    try:
        positions = await get_position_risk(sym)
        for p in positions:
            if p.get("symbol") == sym:
                amt = float(p.get("positionAmt", 0))
                if amt != 0:
                    entry_p = float(p.get("entryPrice", 0))
                    is_long = amt > 0
                    position = {
                        "side": "BUY" if is_long else "SELL",
                        "entry_price": entry_p,
                        "tp_price": entry_p * (1 + tp_pct) if is_long else entry_p * (1 - tp_pct),
                        "sl_price": entry_p * (1 - sl_pct) if is_long else entry_p * (1 + sl_pct),
                        "qty": abs(amt),
                        "upnl": float(p.get("unRealizedProfit", 0)),
                    }
                break
    except Exception:
        pass

    # Tarih filtresi — sadece start_date sonrasi verileri dondur (warmup haric)
    if start_date:
        filtered_candles = [c for c in candles if c["time"] >= start_ms // 1000]
        filtered_signals = [s for s in signals if s["time"] >= start_ms // 1000]
        filtered_trades = [t for t in trades if t.get("entry_time", 0) >= start_ms // 1000]
    else:
        filtered_candles = candles
        filtered_signals = signals
        filtered_trades = trades

    return {
        "candles": filtered_candles,
        "signals": filtered_signals,
        "trades": filtered_trades,
        "position": position,
        "symbol": sym,
        "interval": interval,
        "total_candles": len(filtered_candles),
        "total_signals": len(filtered_signals),
        "total_trades": len(filtered_trades),
        "rsi_state": rsi_state,
        "params": {"rsi_len": rsi_len, "long_thresh": long_thresh, "short_thresh": short_thresh,
                   "max_gap": max_gap, "entry_buffer": entry_buffer, "tp_pct": round(tp_pct*100,2), "sl_pct": round(sl_pct*100,2)},
    }


@router.get("/api/st-signals")
async def api_st_signals(limit: int = 80, symbols: str | None = None) -> dict:
    """Get recent SuperTrend signals — only TradingView webhook signals (entered=1).

    Optional: symbols=BTCUSDT,ETHUSDT to filter by specific symbols.
    """
    from app.modules.st_signal_logger import get_db

    db = await get_db()
    try:
        if symbols:
            sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
            placeholders = ",".join("?" for _ in sym_list)
            cursor = await db.execute(
                f"SELECT * FROM signal_log WHERE entered = 1 AND symbol IN ({placeholders}) ORDER BY id DESC LIMIT ?",  # noqa: S608
                [*sym_list, limit],
            )
        else:
            cursor = await db.execute(
                "SELECT * FROM signal_log WHERE entered = 1 ORDER BY id DESC LIMIT ?",
                [limit],
            )
        rows = await cursor.fetchall()
        signals = [dict(r) for r in rows]
    except Exception:
        signals = []

    return {"signals": signals, "count": len(signals)}


@router.get("/debug/orders")
async def debug_orders() -> dict:
    """Get all Binance orders (regular + algo) for the last 3 days."""
    from app.modules.binance_client import get_all_orders, get_algo_orders_history
    from datetime import datetime, timezone, timedelta
    tz = timezone(timedelta(hours=3))
    try:
        regular_orders, algo_list = await asyncio.gather(
            get_all_orders("ETHUSDT", days=3),
            get_algo_orders_history("ETHUSDT"),
        )

        orders = []
        for o in regular_orders:
            ts = int(o.get("time", 0))
            orders.append({
                "time": datetime.fromtimestamp(ts / 1000, tz=tz).strftime("%Y-%m-%d %H:%M:%S") if ts else "",
                "order_id": str(o.get("orderId", "")),
                "type": o.get("type", ""),
                "side": o.get("side", ""),
                "price": o.get("price", ""),
                "avg_price": o.get("avgPrice", ""),
                "stop_price": o.get("stopPrice", ""),
                "qty": o.get("origQty", ""),
                "filled_qty": o.get("executedQty", ""),
                "status": o.get("status", ""),
                "reduce_only": o.get("reduceOnly", False),
                "source": "regular",
            })

        for o in algo_list:
            ts = int(o.get("bookTime", 0) or o.get("updateTime", 0))
            orders.append({
                "time": datetime.fromtimestamp(ts / 1000, tz=tz).strftime("%Y-%m-%d %H:%M:%S") if ts else "",
                "algo_id": str(o.get("algoId", "")),
                "type": o.get("algoType", "") + "/" + o.get("type", ""),
                "side": o.get("side", ""),
                "trigger_price": o.get("triggerPrice", ""),
                "qty": o.get("origQty", ""),
                "status": o.get("algoStatus", ""),
                "source": "algo",
            })

        orders.sort(key=lambda x: x.get("time", ""), reverse=True)
        return {"orders": orders, "count": len(orders)}
    except Exception as e:
        return {"error": str(e)}


@router.get("/debug/all-open-orders")
async def debug_all_open_orders() -> dict:
    """Get ALL open orders (regular + algo) across all trading symbols."""
    from app.modules.binance_client import get_client, _sign, _raise_for_binance
    from app.config import SYMBOL_CONFIGS

    client = await get_client()
    result: dict = {"regular": [], "algo": [], "positions": []}

    try:
        # 1. All regular open orders (no symbol filter)
        params = _sign({})
        resp = await client.get("/fapi/v1/openOrders", params=params)
        _raise_for_binance(resp)
        result["regular"] = resp.json()
    except Exception as e:
        result["regular_error"] = str(e)

    # 2. Algo orders per symbol
    for sym in SYMBOL_CONFIGS:
        try:
            params = _sign({"symbol": sym})
            resp = await client.get("/fapi/v1/algoOrder/openOrders", params=params)
            data = resp.json()
            orders = data.get("orders", []) if isinstance(data, dict) else []
            for o in orders:
                o["_symbol"] = sym
                result["algo"].append(o)
        except Exception:
            pass

    # 3. Non-zero positions
    for sym in SYMBOL_CONFIGS:
        try:
            params = _sign({"symbol": sym})
            resp = await client.get("/fapi/v2/positionRisk", params=params)
            for p in resp.json():
                if float(p.get("positionAmt", 0)) != 0:
                    result["positions"].append(p)
        except Exception:
            pass

    result["total_regular"] = len(result["regular"])
    result["total_algo"] = len(result["algo"])
    result["total_positions"] = len(result["positions"])
    return result


@router.get("/debug/db-paths")
async def debug_db_paths() -> dict:
    """Show all DB file paths and sizes."""
    import os
    from pathlib import Path
    data_dir = os.getenv("DATA_DIR", "data")
    result = {"DATA_DIR_env": data_dir, "DATA_DIR_abs": os.path.abspath(data_dir), "files": {}}
    # Check known DB files
    for name in ["candles.db", "st_signals.db", "trades.db", "signals.db", "algo_ids.json"]:
        p = Path(data_dir) / name
        if p.exists():
            result["files"][name] = {"path": str(p.resolve()), "size_kb": round(p.stat().st_size / 1024, 1)}
        else:
            result["files"][name] = {"path": str(p.resolve()), "exists": False}
    # Also check /data directly
    data_path = Path("/data")
    if data_path.exists():
        result["/data_contents"] = [f.name for f in data_path.iterdir()][:20]
    return result


@router.get("/debug/reset-directions")
async def debug_reset_directions() -> dict:
    """Reset all direction filter keys so next signal of any direction passes through."""
    from app.modules.redis_client import get_redis
    r = await get_redis()
    deleted = 0
    async for key in r.scan_iter(match="tv:signal_dir:*", count=200):
        await r.delete(key)
        deleted += 1
    return {"deleted_keys": deleted, "message": "Direction filters reset. Next signal will pass through."}


@router.get("/debug/fix-tpsl/{symbol}")
async def debug_fix_tpsl(symbol: str) -> dict:
    """Test: cancel old orders + place new TP/SL for open position."""
    from app.modules.binance_client import (
        cancel_all_open_orders, get_exchange_info, get_position_risk,
        place_stop_market_order, place_take_profit_market_order, round_price,
    )
    from app.config import get_symbol_config
    sym = symbol.upper()
    result: dict = {"symbol": sym}

    try:
        # Get position
        positions = await get_position_risk(sym)
        pos_amt = 0.0
        entry_price = 0.0
        for p in positions:
            if p.get("symbol") == sym:
                pos_amt = float(p.get("positionAmt", 0))
                entry_price = float(p.get("entryPrice", 0))
                break
        result["pos_amt"] = pos_amt
        result["entry_price"] = entry_price

        if pos_amt == 0:
            return {**result, "status": "no_position"}

        is_long = pos_amt > 0
        exit_side = "SELL" if is_long else "BUY"
        qty = abs(pos_amt)
        cfg = get_symbol_config(sym)
        tp_pct = cfg.get("tp_pct", 0.005)
        sl_pct = cfg.get("sl_pct", 0.015)

        if is_long:
            raw_tp = entry_price * (1 + tp_pct)
            raw_sl = entry_price * (1 - sl_pct)
        else:
            raw_tp = entry_price * (1 - tp_pct)
            raw_sl = entry_price * (1 + sl_pct)

        info = await get_exchange_info(sym)
        tick_size = info["priceFilter"]["tickSize"]
        tp_price = round_price(raw_tp, tick_size)
        sl_price = round_price(raw_sl, tick_size)

        result["tp_pct"] = tp_pct
        result["sl_pct"] = sl_pct
        result["tp_price"] = tp_price
        result["sl_price"] = sl_price
        result["exit_side"] = exit_side
        result["qty"] = qty

        # Cancel existing
        cancel_result = await cancel_all_open_orders(sym)
        result["cancelled"] = cancel_result

        # Place TP (algoOrder API)
        try:
            tp_resp = await place_take_profit_market_order(sym, exit_side, qty, tp_price)
            result["tp_order"] = tp_resp
            result["tp_ok"] = True
        except Exception as e:
            result["tp_error"] = str(e)
            result["tp_ok"] = False
            import traceback
            result["tp_traceback"] = traceback.format_exc()

        # Place SL (algoOrder API)
        try:
            sl_resp = await place_stop_market_order(sym, exit_side, qty, sl_price)
            result["sl_order"] = sl_resp
            result["sl_ok"] = True
        except Exception as e:
            result["sl_error"] = str(e)
            result["sl_ok"] = False
            import traceback
            result["sl_traceback"] = traceback.format_exc()

        result["status"] = "done"
    except Exception as e:
        result["error"] = str(e)
        import traceback
        result["traceback"] = traceback.format_exc()
    return result


@router.get("/debug/raw-open-orders/{symbol}")
async def debug_raw_open_orders(symbol: str) -> dict:
    """Raw Binance open orders debug — her turlu emri goster."""
    from app.modules.binance_client import get_client, _sign, _raise_for_binance
    client = await get_client()
    sym = symbol.upper()
    result: dict = {"symbol": sym}

    # 1. Regular open orders (symbol specific)
    try:
        params = _sign({"symbol": sym})
        resp = await client.get("/fapi/v1/openOrders", params=params)
        _raise_for_binance(resp)
        result["regular_orders"] = resp.json()
    except Exception as e:
        result["regular_error"] = str(e)

    # 2. Algo open orders (all symbols — API might not support symbol filter)
    try:
        params = _sign({})
        resp = await client.get("/fapi/v1/algoOrder/openOrders", params=params)
        resp.raise_for_status()
        raw = resp.json()
        result["algo_raw_response"] = raw
        orders = raw.get("orders", []) if isinstance(raw, dict) else raw
        result["algo_all"] = orders
        result["algo_for_symbol"] = [o for o in orders if o.get("symbol") == sym]
    except Exception as e:
        result["algo_error"] = str(e)

    # 3. Algo with symbol param
    try:
        params = _sign({"symbol": sym})
        resp = await client.get("/fapi/v1/algoOrder/openOrders", params=params)
        resp.raise_for_status()
        result["algo_with_symbol"] = resp.json()
    except Exception as e:
        result["algo_with_symbol_error"] = str(e)

    # 4. All open orders (no symbol filter) — catch everything
    try:
        params = _sign({})
        resp = await client.get("/fapi/v1/openOrders", params=params)
        _raise_for_binance(resp)
        all_orders = resp.json()
        result["all_open_orders_count"] = len(all_orders)
        result["all_open_orders"] = all_orders
    except Exception as e:
        result["all_open_orders_error"] = str(e)

    # 5. allOrders son 1 gun — status=NEW olan conditional emirler
    try:
        import time
        start_time = int((time.time() - 86400) * 1000)
        params = _sign({"symbol": sym, "startTime": start_time, "limit": 50})
        resp = await client.get("/fapi/v1/allOrders", params=params)
        _raise_for_binance(resp)
        all_hist = resp.json()
        active = [o for o in all_hist if o.get("status") == "NEW"]
        result["allOrders_NEW"] = active
        result["allOrders_total"] = len(all_hist)
    except Exception as e:
        result["allOrders_error"] = str(e)

    # 6. openOrder conditional (fapi v2 — bazi Binance versiyonlari)
    for endpoint in ["/fapi/v1/openOrder", "/fapi/v2/openOrders"]:
        try:
            params = _sign({"symbol": sym})
            resp = await client.get(endpoint, params=params)
            if resp.is_success:
                result[f"test_{endpoint}"] = resp.json()
        except Exception:
            pass

    return result
