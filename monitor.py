"""Canli sinyal ve trade takip scripti (Railway API + Binance public)."""

import asyncio
import json
import os
import time
import urllib.request
from urllib.parse import urlencode

WEBHOOK_URL = "https://web-production-25e45.up.railway.app"
REFRESH = 2
SIGNAL_REFRESH = 10
INITIAL_BALANCE = 48.99  # Baslangic bakiyesi (USDT)
BASE_URL = "https://fapi.binance.com"


def _http_get(url: str) -> dict | list:
    """Basit GET — imzasiz."""
    req = urllib.request.Request(url, headers={"User-Agent": "monitor/1.0"})
    with urllib.request.urlopen(req, timeout=8) as resp:
        return json.loads(resp.read())


def get_mark_price() -> float:
    """Binance public endpoint — auth gerektirmez."""
    data = _http_get(f"{BASE_URL}/fapi/v1/premiumIndex?symbol=ETHUSDT")
    return float(data.get("markPrice", 0))


def get_diagnosis() -> dict:
    """Railway diagnosis endpoint — hesap bilgisi, pozisyon, trade'ler."""
    return _http_get(f"{WEBHOOK_URL}/debug/diagnosis")


def get_recent_signals() -> list[dict]:
    data = _http_get(f"{WEBHOOK_URL}/events?symbol=ETHUSDT&limit=10")
    return data.get("events", [])


def get_income() -> dict:
    """Railway income endpoint — gerceklesmis kar/zarar gecmisi."""
    return _http_get(f"{WEBHOOK_URL}/debug/income")


def clear():
    print("\033[2J\033[H", end="")


def c(text, code):
    return f"\033[{code}m{text}\033[0m"


def bar(pct, width=20, scale=5):
    filled = int(abs(pct) / scale * width)
    filled = max(1 if abs(pct) > 0.01 else 0, min(filled, width))
    if pct >= 0:
        return c("[" + "#" * filled + "." * (width - filled) + "]", "92")
    else:
        return c("[" + "#" * filled + "." * (width - filled) + "]", "91")


async def monitor():
    seen = set()
    trade_log = []
    first_run = True
    cached_signals = []
    cached_diag = {}
    cached_income = {}
    last_signal_fetch = 0
    last_diag_fetch = 0
    last_income_fetch = 0

    print(c("Canli takip baslatiliyor...\n", "1"))

    while True:
        try:
            now = time.strftime("%H:%M:%S")

            # Sinyalleri yavas cek
            if time.time() - last_signal_fetch >= SIGNAL_REFRESH:
                try:
                    cached_signals = await asyncio.to_thread(get_recent_signals)
                    last_signal_fetch = time.time()
                except Exception:
                    pass

            # Diagnosis (hesap, pozisyon, emirler) yavas cek
            if time.time() - last_diag_fetch >= SIGNAL_REFRESH:
                try:
                    cached_diag = await asyncio.to_thread(get_diagnosis)
                    last_diag_fetch = time.time()
                except Exception:
                    pass

            # Income (kar/zarar gecmisi) 30sn'de bir cek
            if time.time() - last_income_fetch >= 30:
                try:
                    cached_income = await asyncio.to_thread(get_income)
                    last_income_fetch = time.time()
                except Exception:
                    pass

            # Mark price her saniye (public endpoint, auth gerektirmez)
            mark = await asyncio.to_thread(get_mark_price)

            # Diagnosis verilerini parse et
            balance = cached_diag.get("balance", 0)
            pos_data = cached_diag.get("position", {})
            pos_amt = float(pos_data.get("positionAmt", 0))
            entry_price = float(pos_data.get("entryPrice", 0))
            config = cached_diag.get("config", {})
            strategies = config.get("strategies", {})

            # Pozisyon bilgisi
            pos = None
            live_pnl = 0.0
            if pos_amt != 0:
                if pos_amt > 0:
                    live_pnl = (mark - entry_price) * pos_amt
                else:
                    live_pnl = (entry_price - mark) * abs(pos_amt)
                notional = abs(pos_amt) * mark
                pos = {
                    "direction": "SHORT" if pos_amt < 0 else "LONG",
                    "size": abs(pos_amt),
                    "entry": entry_price,
                    "pnl": live_pnl,
                    "notional": notional,
                }

            total_equity = balance + live_pnl + (abs(pos_amt) * entry_price if pos_amt != 0 else 0)
            net_pnl = total_equity - INITIAL_BALANCE
            net_pnl_pct = (net_pnl / INITIAL_BALANCE) * 100

            clear()

            # Baslik
            print(c("=" * 60, "36"))
            eq_color = "92" if net_pnl >= 0 else "91"
            print(c("  ETHUSDT CANLI TAKIP", "1;36") + "             " + c(f"${total_equity:.2f}", f"1;{eq_color}"))
            print(c(f"  {time.strftime('%Y-%m-%d %H:%M:%S')}", "36") + f"              ETH: ${mark:.2f}")
            print(c("=" * 60, "36"))

            # Hesap durumu
            print(c("\n  HESAP DURUMU", "1;33"))
            pnl_sign = "+" if net_pnl >= 0 else ""
            pnl_color = "92" if net_pnl >= 0 else "91"
            print(f"  Baslangic:     ${INITIAL_BALANCE:.2f}")
            print(f"  Toplam Deger:  {c(f'${total_equity:.2f}', f'1;{eq_color}')}")
            print(f"  Net Kar/Zarar: {c(f'{pnl_sign}${net_pnl:.4f} ({pnl_sign}{net_pnl_pct:.2f}%)', pnl_color)}")
            print(f"  {bar(net_pnl_pct)}")
            print(f"  Serbest:       ${balance:.2f}")

            # Strateji bilgisi
            if strategies:
                print(c("\n  STRATEJILER", "1;33"))
                for tf, vals in strategies.items():
                    sl = vals.get("sl_pct", 0) * 100
                    tp = vals.get("tp_pct", 0) * 100
                    print(f"  {tf}: SL %{sl:.2f} | TP %{tp:.2f}")

            # Pozisyon
            print(c("\n  POZISYON", "1;33"))
            if pos:
                dir_color = "91" if pos["direction"] == "SHORT" else "92"
                pos_pnl = pos["pnl"]
                pos_pnl_pct = (pos_pnl / pos["notional"]) * 100 if pos["notional"] > 0 else 0
                pos_pnl_color = "92" if pos_pnl >= 0 else "91"
                pos_pnl_sign = "+" if pos_pnl >= 0 else ""

                print(f"  {c(pos['direction'], f'1;{dir_color}')} {pos['size']} ETH  |  " +
                      f"Giris: ${pos['entry']:.2f}  |  Mark: ${mark:.2f}")
                print(f"  PnL: {c(f'{pos_pnl_sign}${pos_pnl:.4f} ({pos_pnl_sign}{pos_pnl_pct:.2f}%)', pos_pnl_color)}")

                # SL/TP bilgisi (son trade'den)
                trades = cached_diag.get("recent_trades", [])
                if trades:
                    last_trade = trades[0]
                    sl_price = last_trade.get("stop_price")
                    if sl_price:
                        sl_dist = abs(mark - sl_price)
                        sl_pct = (sl_dist / mark) * 100
                        print(f"  Stop-Loss: ${sl_price:.2f} " + c(f"[{sl_pct:.2f}% uzakta]", "91"))
            else:
                print("  Acik pozisyon yok")

            # Trade gecmisi (gerceklesmis kar/zarar)
            if cached_income and not cached_income.get("error"):
                inc_trades = cached_income.get("trades", [])
                total_rpnl = cached_income.get("total_pnl", 0)
                win = cached_income.get("win", 0)
                lose = cached_income.get("lose", 0)
                win_rate = cached_income.get("win_rate", "0%")
                rpnl_color = "92" if total_rpnl >= 0 else "91"
                rpnl_sign = "+" if total_rpnl >= 0 else ""

                print(c("\n  TRADE GECMISI (7 gun)", "1;33"))
                print(f"  Toplam: {c(f'{rpnl_sign}${total_rpnl:.4f}', rpnl_color)}  |  " +
                      c(f"{win}W", "92") + f"/{c(f'{lose}L', '91')}  |  Oran: {win_rate}")
                print(c(f"  {'Zaman':<20} {'Kar/Zarar':>12}", "37"))
                print(c("  " + "-" * 34, "36"))
                for t in inc_trades[:8]:
                    pnl = t["pnl"]
                    p_color = "92" if pnl >= 0 else "91"
                    p_sign = "+" if pnl >= 0 else ""
                    tm = t["time"][5:]  # MM-DD HH:MM:SS
                    print(f"  {tm:<20} {c(f'{p_sign}${pnl:.4f}', p_color):>24}")

            # Son sinyaller
            print(c("\n  SON SINYALLER", "1;33"))
            header = f"  {'Zaman':<20} {'Sinyal':<7} {'Indikator':<20} {'TF':<4} {'Fiyat':>10}"
            print(c(header, "37"))
            print(c("  " + "-" * 58, "36"))

            for s in cached_signals[:6]:
                sig = s.get("signal", "?")
                sig_color = "92" if sig == "BUY" else "91"
                ts = s.get("ts_human", "")[:19]
                ind = s.get("indicator", "?")[:18]
                tf = s.get("tf", "?")
                price = s.get("price", 0)
                print(f"  {ts:<20} {c(f'{sig:<7}', sig_color)} {ind:<20} {tf:<4} ${price:>9.2f}")

                eid = s.get("event_id")
                if eid and eid not in seen:
                    if not first_run:
                        trade_log.append(f"[{now}] {sig} @ ${price} ({ind} {tf})")
                    seen.add(eid)

            first_run = False

            # Trade log
            if trade_log:
                print(c("\n  YENI SINYALLER", "1;35"))
                for t in trade_log[-5:]:
                    print(f"  >> {t}")

            print(c(f"\n{'=' * 60}", "36"))
            print(c("  Ctrl+C ile durdur  |  Her 2sn yenilenir", "36"))

            await asyncio.sleep(REFRESH)

        except KeyboardInterrupt:
            print(c("\n\nTakip durduruldu.", "33"))
            break
        except Exception as e:
            print(f"\n[HATA] {e}")
            await asyncio.sleep(REFRESH)


if __name__ == "__main__":
    asyncio.run(monitor())
