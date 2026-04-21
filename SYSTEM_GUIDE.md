# Trading Sistemi - Teknik Dokumantasyon

## Genel Bakis

Binance Futures otomatik trading botu. Heikin-Ashi Reversal + RSI Yon Filtresi stratejisi ile 2 Binance hesabi (ping-pong) uzerinden islem yapar.

- **Backend:** Python 3.13, FastAPI, asyncio
- **Deploy:** Railway (https://web-production-25e45.up.railway.app)
- **Borsa:** Binance Futures (gercek hesap, testnet degil)
- **Frontend:** Vanilla HTML/JS (static/ klasoru)

---

## HA Sinyal Motoru (ha_signal_engine.py)

Ana trading motoru. Tum islem acma/kapama kararlarini bu motor verir.

### Strateji: HA Reversal + RSI Sinyal Yon Filtresi

**Indikatör:** Pine Script "HA Reversal + RSI Sinyal Yonu Filtresi"

#### Giris Kosullari

| Yon | Kosul | Aciklama |
|-----|-------|----------|
| LONG | `bullSignal[1] AND rsiUp` | Onceki mumda haOpen==haLow VE RSI yukseliyor |
| SHORT | `bearSignal[1] AND rsiDown` | Onceki mumda haOpen==haHigh VE RSI dususyor |

- `bullSignal`: HA mumda alt golge yok (haOpen == haLow) → guclu yukselis
- `bearSignal`: HA mumda ust golge yok (haOpen == haHigh) → guclu dusus
- `rsiUp`: Bu mumun HA RSI(10) > onceki mumun HA RSI(10)
- `rsiDown`: Bu mumun HA RSI(10) < onceki mumun HA RSI(10)
- RSI ters yondeyse sinyal iptal (duserken LONG acma, yukselirken SHORT acma)
- Tolerans: fiyatin %0.01'i (haOpen vs haLow/haHigh karsilastirmasi)

#### Cikis Kosullari

| Yon | Kosul | Aciklama |
|-----|-------|----------|
| LONG kapat | `rsiDown` | Mum kapanisinda RSI dustuyse (momentum kaybi) |
| SHORT kapat | `rsiUp` | Mum kapanisinda RSI yükseldiyse (momentum kaybi) |
| Ters sinyal | REVERSE | Ters HA reversal + uygun RSI yonu gelirse eski kapat + yeni ac |

- Giris mumunda cikis yok (en az 1 mum acik kalmali)
- Canli (tick bazli) RSI exit YOK — sadece mum kapanisinda kontrol
- Guvenlik SL: Giris fiyatindan %3 uzaklikta STOP_MARKET (ayarlanabilir)

#### RSI Detaylari

- Kaynak: HA close (Heikin-Ashi kapanış fiyati)
- Periyot: 10 (Wilder's Smoothed MA / RMA)
- Karsilastirma: `closed_rsi` vs `prev_closed_rsi` (onceki mumun RSI'i)
- Sabit threshold YOK (65/35 gibi esikler kaldirildi)

### Akis Diyagrami

```
[Her 1 saniye — price tick]
    |
    v
on_price_tick(price)
    |
    ├── Canli mum OHLC guncelle
    ├── HA canli mum guncelle (_update_ha_candle)
    |
    ├── Mum kapandi mi? (new_candle_start > candle_start)
    |   |
    |   ├── EVET:
    |   |   ├── Binance'tan gercek OHLC cek (limit=2)
    |   |   ├── _calc_ha(real_ohlc, prev_ha_open, prev_ha_close)
    |   |   ├── HA RSI hesapla (closed_rsi)
    |   |   ├── RSI yon tespit (rsiUp / rsiDown)
    |   |   ├── HA Reversal sinyal tespit (bullSignal / bearSignal)
    |   |   |
    |   |   ├── RSI Yon Exit kontrolu:
    |   |   |   ├── LONG + rsiDown → kapat (RSI_DOWN)
    |   |   |   └── SHORT + rsiUp → kapat (RSI_UP)
    |   |   |   (giris mumunda cikis yok)
    |   |   |
    |   |   ├── prev_closed_rsi guncelle
    |   |   └── Yeni mum baslat, signal_fired_this_bar = False
    |   |
    |   └── HAYIR: devam
    |
    ├── signal_fired_this_bar? → skip
    |
    └── Sinyal kontrolu:
        ├── prev_bull_signal AND rsiUp → BUY signal dondur
        ├── prev_bear_signal AND rsiDown → SELL signal dondur
        └── Hicbiri → None
```

```
[_ha_engine_loop — sinyal geldikten sonra]
    |
    v
Signal alindi
    |
    ├── st_signal_logger'a kaydet (source=ha_server)
    ├── trading_enabled kontrol
    |
    ├── Pre-trade sync (Binance'tan A/B gercek pozisyon)
    |
    ├── Ters pozisyonlari kapat (REVERSE)
    |
    ├── Bos hesap bul:
    |   ├── A bos → A'dan ac
    |   ├── A dolu, B bos → B'den ac
    |   ├── Ikisi dolu ayni yon → en eski kapat (ROTATION) + ac
    |   └── Tek hesap dolu → kapat + yeniden ac (SINGLE_REOPEN)
    |
    └── _open_account_position(hesap, yon)
        ├─��� Bakiye cek
        ├── Quantity = bakiye × weight × 0.98 / fiyat
        ├── Market order gonder
        ├── State guncelle (side, entry, qty, time, entry_bar)
        └── Guvenlik SL koy (%3 STOP_MARKET)
```

### 2 Hesap Ping-Pong Sistemi

Ayni anda 2 Binance hesabindan (A + B) pozisyon acilebilir.

**Kullanim senaryosu:**
```
15:00 → SHORT sinyal → Hesap A'dan SHORT ac
15:30 → Tekrar SHORT sinyal → Hesap A dolu → Hesap B'den SHORT ac
15:45 → RSI yon exit → Hesap A kapat (en eski)
16:00 → RSI yon exit → Hesap B kapat
```

**State yonetimi:** `_account_state` dict (bellekte):
```python
{
    "ETHUSDT": {
        "a": {"side": "SHORT", "entry": 2315.0, "qty": 0.074, "time": 1713..., "entry_bar": 1713...},
        "b": {"side": None, "entry": 0.0, "qty": 0.0, "time": 0}
    }
}
```

**Hesap B konfigurasyonu:** `.env`'de `BINANCE_API_KEY_B` ve `BINANCE_API_SECRET_B` tanimli olmali.

### Pozisyon Kapama (_close_account_position)

1. Binance'tan gercek pozisyon kontrol (`get_position_risk` / `get_position_risk_b`)
2. `pos_amt == 0` → zaten kapanmis, state temizle
3. Acik emirleri iptal (`cancel_all_open_orders` / `cancel_all_open_orders_b` — algo order'lar dahil)
4. Market reduceOnly order
5. PnL hesapla ve logla
6. st_signal_logger'a kapanış kaydı (`source=ha_close_{reason}`)
7. State temizle (SADECE market order basarili olursa — exception'da state'e dokunma)

**Kapanış sebepleri:**
- `RSI_DOWN` — LONG pozisyon, RSI dustuyse
- `RSI_UP` — SHORT pozisyon, RSI yükseldiyse
- `REVERSE` — Ters sinyal geldi
- `ROTATION` — Ikisi dolu, en eski kapatildi
- `SINGLE_REOPEN` — Tek hesap, kapat + yeniden ac

### Warmup

Startup'ta her sembol icin:
1. Binance'tan 1500 mum cek (simülasyonla ayni sayi — HA drift onleme)
2. `convert_klines_to_ha()` ile HA'ya donustur
3. HA close'lardan RSI(10) hesapla (Wilder's RMA)
4. RSI state baslat (`rsi_avg_gain`, `rsi_avg_loss`, `rsi_prev_close`)
5. `prev_closed_rsi` son kapanan mumdan set et
6. HA state baslat (`ha_prev_open`, `ha_prev_close`)
7. Canli mum baslat (son mumun OHLC)

### Periyodik Gorevler

| Gorev | Aralik | Aciklama |
|-------|--------|----------|
| Fiyat tick | 1sn | WebSocket'ten fiyat al, `on_price_tick` cagir |
| Pozisyon sync | 300sn (5dk) | Binance'tan A/B gercek durumu `_account_state`'e yaz |
| Settings reload | 60sn | Yeni sembol ekle/kaldir, ayarlari guncelle |
| Startup sync | 1 kez | Deploy sonrasi Binance'tan A/B pozisyon yukle |

---

## Izolasyon — HA Motor Bagimsizligi

HA motor SignalEngine'den miras alir ama kendi state yonetimini yapar. Disaridan mudahale engellenmistir:

| Modul | Izolasyon |
|-------|-----------|
| `order_stream.py` | HA sembollerinde sadece log, state degisikligi yok |
| `SignalEngine._sync_position` | Override → NO-OP |
| `SignalEngine.on_position_closed` | Override → NO-OP |
| `SignalEngine.on_position_opened` | Override → NO-OP |
| `trailing_tp.py` | HA sembollerini atliyor |
| `data_collector.py` | `ha_enabled` semboller icin divergence trade acmiyor |
| `st_webhook.py` | Webhook trade devre disi (`if False`) — sadece loglar |

---

## Hesap B — Algo Order Tracking

Hesap A: `place_stop_market_instant()` → `_track_algo_id_async()` → `algo_ids.json`
Hesap B: `place_stop_market_instant_b()` → `_track_algo_id_b_async()` → `algo_ids_b.json`

`cancel_all_open_orders_b()` hem regular hem algo order'lari siler.

---

## Konfigürasyon

### .env (Railway)

| Degisken | Deger | Aciklama |
|----------|-------|----------|
| `TRADING_ENABLED` | `true` | Kill-switch |
| `BINANCE_TESTNET` | `false` | Gercek Binance |
| `BINANCE_API_KEY` | ... | Hesap A |
| `BINANCE_API_SECRET` | ... | Hesap A |
| `BINANCE_API_KEY_B` | ... | Hesap B |
| `BINANCE_API_SECRET_B` | ... | Hesap B |

### indicator_settings.db (Sembol bazli)

| Alan | Aciklama |
|------|----------|
| `active` | Sembol aktif mi |
| `ha_enabled` | HA motor bu sembol icin calissin mi |
| `listening` | Webhook dinleme (HA'da kullanilmiyor) |
| `sl_enabled` | Guvenlik SL koy mu |
| `sl_pct` | SL yuzde (default 3.0) |
| `weight` | Bakiye yuzde orani (0.01 = %1) |
| `interval` | Mum periyodu (15m default) |
| `rsi_len` | RSI periyodu (10 default — gap icin, HA RSI ayri) |
| `weekend_closed` | Haftasonu islem kapatma (XAG gibi TradFi) |

---

## Frontend Sayfalari

| Sayfa | URL | Aciklama |
|-------|-----|----------|
| Trading | `/trading` | Ana dashboard, sembol kartlari, pozisyon yonetimi, toggle'lar |
| HA Monitor | `/ha-monitor` | HA mum gecmisi, simulasyon, motor sinyalleri, canli RSI |
| Monitor | `/monitor` | Normal motor (eski, kullanilmiyor) |
| Settings | `/settings` | Sembol ayarlari |
| Piyasa | `/` | Genel piyasa gorunumu |

### HA Monitor Simulasyonu

`runReversalSim()` fonksiyonu (ha_monitor.html) motorla birebir ayni mantigi calistirir:
- HA mumlardan bullSignal/bearSignal tespit
- RSI yon filtresi (rsiUp/rsiDown)
- Giris: `bullSignal[1] AND rsiUp` / `bearSignal[1] AND rsiDown`
- Cikis: LONG → rsiDown, SHORT → rsiUp
- Giris mumunda cikis yok
- Simülasyon sonuclari mum tablosunda gösterilir

### Motor Sinyalleri Mum Gecmisinde

st_signal_logger'a kaydedilen motor islemleri mum gecmisinde gösterilir:
- **Giris:** `▲ MOTOR LONG` / `▼ MOTOR SHORT` (source=ha_server)
- **Cikis:** `✕ CLOSE RSI_DOWN` / `✕ CLOSE RSI_UP` / `✕ CLOSE REVERSE` (source=ha_close_*)

---

## API Endpointleri (HA Motor ile ilgili)

| Endpoint | Aciklama |
|----------|----------|
| `GET /api/signal-engine-status` | Motor durumu, ha_engines listesi |
| `GET /api/ha-live-rsi?symbol=X` | Canli HA RSI degeri |
| `GET /api/ha-chart-data?symbol=X` | HA mum + RSI verileri (simulasyon icin) |
| `GET /api/st-signals?symbol=X` | Sinyal gecmisi (giris + cikis) |
| `GET /api/indicator-settings` | Tum sembol ayarlari |
| `PUT /api/indicator-settings/{symbol}` | Sembol ayari guncelle |
| `GET /api/order-history?symbol=X` | Binance order gecmisi (sadece Hesap A) |
| `GET /api/binance-trades?symbol=X` | Binance trade gecmisi |

---

## Bilinen Sinirlamalar

1. **HA Drift:** Motor warmup'ta 1500 mum ceker, simulasyon da 1500. Farkli zamanlarda baslayan motorlar arasi kucuk HA farklari olabilir.
2. **Order History:** `/api/order-history` sadece Hesap A'yi gosteriyor. Hesap B emirleri gorunmuyor.
3. **Order Stream:** Sadece Hesap A icin WebSocket dinleniyor. Hesap B'nin SL tetiklenmesi order_stream'e dusmuyor.
4. **Pozisyon Sync:** 5dk arayla — arada Binance'ta manuel islem yapilirsa state gecikmeli guncellenir.
