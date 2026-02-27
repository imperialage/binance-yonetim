# Market Intelligence Service

TradingView'den gelen çoklu indikatör + çoklu timeframe webhook alert'lerini tek endpoint'te toplayıp, deterministik kurallarla **decision / bias / confidence** üreten ve AI'ye sadece bu kararı **açıklattıran** mikro servis.

> **Emir gönderme (execution) yoktur.** Bu servis sadece sinyal toplama, değerlendirme ve raporlama yapar.

---

## Mimari

```
TradingView Alerts ──► POST /tv-webhook ──► Validate ──► Dedupe ──► Normalize
                                                                        │
                                              ┌─────────────────────────┘
                                              ▼
                                        Redis Store (RPUSH + LTRIM)
                                              │
                              ┌───────────────┼───────────────┐
                              ▼               ▼               ▼
                         Aggregation    Rules Engine    Market Data (Binance)
                              │               │               │
                              └───────┬───────┘               │
                                      ▼                       │
                                 AI Explainer ◄───────────────┘
                                      │  (single-flight lock)
                                      ▼
                              Redis (tv:latest:{symbol})
                                      │
                              GET /latest?symbol=ETHUSDT

                        ┌─────────────────────────────┐
                        │    Scheduled Refresher       │
                        │  (her 30s rules, 120s AI)    │
                        │  watchlist symbols döngüsü   │
                        └──────────────┬──────────────┘
                                       ▼
                              tv:latest:{symbol}
```

## Kurulum

```bash
# 1. Python 3.11+ ve Redis gerekli
# 2. Bağımlılıkları yükle
pip install -r requirements.txt

# 3. .env dosyası oluştur
cp .env.example .env
# .env içindeki TV_WEBHOOK_SECRET ve ADMIN_TOKEN değerlerini değiştir

# 4. Redis başlat
redis-server

# 5. Servisi başlat
python run.py
# veya: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Endpoint'ler

| Metod | Path | Açıklama |
|-------|------|----------|
| POST | `/tv-webhook` | TradingView webhook alıcı |
| GET | `/status` | Health check + metrikler |
| GET | `/latest?symbol=ETHUSDT` | Son iki-katmanlı evaluation (rules + AI) |
| GET | `/events?symbol=ETHUSDT` | Event geçmişi (filtrelenebilir) |
| POST | `/config` | Runtime config güncelle (X-Admin-Token header) |

---

## Scheduler (Zamanlanmış Güncelleme)

**Problem:** Evaluation sadece webhook gelince güncelleniyordu; panel uzun süre "bayat" kalabiliyordu.

**Çözüm:** `app/modules/scheduler.py` — FastAPI lifespan içinde çalışan async background task.

### Nasıl Çalışır

1. Uygulama başlatılınca `start_scheduler()` asyncio task oluşturur.
2. Her `refresh_rules_seconds` (default 30s) saniyede bir watchlist'teki tüm semboller için:
   - Redis'ten event'leri çeker
   - Aggregation + Rules engine çalıştırır
   - Market data (Binance klines) getirir
   - `tv:latest:{symbol}` günceller
3. Her `refresh_ai_seconds` (default 120s) saniyede bir veya decision değişince AI explanation üretir.
4. Shutdown sinyalinde task düzgünce iptal edilir (`CancelledError` handling).

### Rules vs AI Refresh Farkı

| Katman | Sıklık | Neden |
|--------|--------|-------|
| **Rules** | Her 30s | Mevcut sinyaller + market data ile hızlı deterministik değerlendirme |
| **AI** | Her 120s | Yavaş ve pahalı; sadece gerektiğinde (decision değişimi veya zamanlı) |

Bu iki-katmanlı yaklaşım `/latest` çıktısına yansır:
- `latest_rules`: sık güncellenir (decision, bias, confidence, score, reasons)
- `latest_ai`: seyrek güncellenir (6 satırlık Türkçe açıklama)

---

## /events Endpoint

```bash
# Basit kullanım
curl "http://localhost:8000/events?symbol=ETHUSDT"

# Filtreli
curl "http://localhost:8000/events?symbol=ETHUSDT&limit=20&indicator=BigBeluga&tf=1h&signal=BUY"
```

**Parametreler:**

| Param | Zorunlu | Default | Açıklama |
|-------|---------|---------|----------|
| `symbol` | Evet | - | Sembol (ETHUSDT) |
| `limit` | Hayır | 50 | Max 500 |
| `indicator` | Hayır | - | Filtre: indikatör adı |
| `tf` | Hayır | - | Filtre: timeframe (15m/1h/4h) |
| `signal` | Hayır | - | Filtre: BUY/SELL/CLOSE/NEUTRAL |

Response en yeni event'ler önce sıralı gelir. Secret alanları çıkarılır.

---

## Event Store & LTRIM

**Problem:** `tv:events:{symbol}` Redis list'i sınırsız büyüyebilir.

**Çözüm:** Her event yazımından sonra `LTRIM` ile son N event tutulur (default 1000, `events_max_per_symbol` ile ayarlanabilir).

```
RPUSH tv:events:ETHUSDT <event_json>
LTRIM tv:events:ETHUSDT -1000 -1
EXPIRE tv:events:ETHUSDT 86400
```

**Gerekçe:** Aggregation yalnızca son birkaç dakikadaki event'leri kullanır (15m pencere: 5dk, 1h: 15dk, 4h: 30dk). 1000 event, en yoğun akışta bile birkaç saatlik geçmişi tutar. Bu, Redis bellek kullanımını öngörülebilir kılar.

---

## AI Single-Flight Lock

**Problem:** Aynı symbol için arka arkaya webhook geldiğinde birden çok AI task aynı anda `/latest` yazabilir.

**Çözüm:** Redis-based distributed lock (`SET key value NX PX`):

```
Key:    tv:lock:ai:{symbol}
TTL:    60 saniye (auto-expire)
```

- Lock alınabilirse AI explanation üretilir
- Lock meşgulse AI üretimi atlanır (rules result yine yazılır)
- Lock, Lua script ile compare-and-delete yapılarak güvenli şekilde serbest bırakılır
- Kuyruk yok — at-most-once semantiği

`/latest` yazarken timestamp karşılaştırması yapılır: eski evaluation yeni olanı ezmez.

---

## Timeframe Normalization

TradingView alert'leri farklı formatlarda tf gönderebilir. Normalizer otomatik çevirir:

| Giriş | Çıkış |
|-------|-------|
| `15`, `15m` | `15m` |
| `60`, `1h`, `1H` | `1h` |
| `240`, `4h`, `4H` | `4h` |
| Diğer | 400 Bad Request |

---

## TradingView Webhook Ayarı

> **ÖNEMLİ:** BigBeluga, ChartPrime, SwiftAlgo **indicator (study)** dir, strategy değildir.
> `{{strategy.order.action}}` **ÇALIŞMAZ**. Signal alanı her alert'te **manuel** set edilir.

### Webhook URL
```
https://your-server.com/tv-webhook
```

### Her indikatör + TF için **2 alert** kurulur (BUY ve SELL ayrı):

**BUY Alert Message:**
```json
{"secret":"SIFRENIZ","indicator":"BigBeluga","symbol":"{{ticker}}","tf":"{{interval}}","signal":"BUY","price":"{{close}}","ts":"{{timenow}}"}
```

**SELL Alert Message:**
```json
{"secret":"SIFRENIZ","indicator":"BigBeluga","symbol":"{{ticker}}","tf":"{{interval}}","signal":"SELL","price":"{{close}}","ts":"{{timenow}}"}
```

3 indikatör × 3 TF × 2 yön = **18 alert** (sadece `indicator` alanı ve `signal` değişir).

### Servis otomatik normalize eder:
- `{{ticker}}` → `"BINANCE:ETHUSDT"` → `"ETHUSDT"` (prefix temizlenir)
- `{{interval}}` → `"60"` → `"1h"` (TF normalize edilir)
- `{{close}}` → `"1856.02"` → `1856.02` (string→float)
- `{{timenow}}` → `"1771973549"` → `1771973549` (string→int)

## Kurallar Motoru

- **TF ağırlıkları**: 4h=0.5, 1h=0.3, 15m=0.2
- **Skor**: Σ(direction × tfWeight × indWeight × strength)
- **Eşik**: 0.35 (runtime'da değiştirilebilir)
- **Bias**: score ≥ threshold → LONG, score ≤ -threshold → SHORT, else NEUTRAL
- **Veto**: 4H SELL iken LONG_SETUP üretilmez (NO_TRADE)
- **Karar**: LONG_SETUP / SHORT_SETUP / WATCH / NO_TRADE

## Runtime Config

```bash
curl -X POST http://localhost:8000/config \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: your_admin_token" \
  -d '{
    "watchlist_symbols": ["ETHUSDT", "BTCUSDT"],
    "refresh_rules_seconds": 30,
    "refresh_ai_seconds": 120,
    "events_max_per_symbol": 1000,
    "tf_weights": {"4h": 0.5, "1h": 0.3, "15m": 0.2},
    "indicator_weights": {"BigBeluga": 0.8, "ChartPrime": 1.0, "SwiftAlgo": 0.9},
    "threshold": 0.35,
    "tf_windows": {"15m": 300, "1h": 900, "4h": 1800}
  }'
```

## Test

```bash
pytest -v
```

### Test Senaryoları (22 test)

| # | Senaryo | Dosya |
|---|---------|-------|
| 1 | Geçerli webhook accept | test_webhook.py |
| 2 | Yanlış secret → 401 | test_webhook.py |
| 3 | Aynı event_id dedupe | test_webhook.py |
| 4 | Eksik alan 422 | test_webhook.py |
| 5 | Rules LONG_SETUP | test_webhook.py |
| 6 | Rules 4H veto | test_webhook.py |
| 7 | Rules NEUTRAL/WATCH | test_webhook.py |
| 8 | /events basic | test_enhancements.py |
| 9 | /events filters | test_enhancements.py |
| 10 | LTRIM 1000 limit | test_enhancements.py |
| 11 | Scheduler tick | test_enhancements.py |
| 12 | AI single-flight lock | test_enhancements.py |
| 13 | TF normalization unit | test_enhancements.py |
| 14 | Invalid TF → 400 | test_enhancements.py |
| 15 | Symbol prefix normalize ("BINANCE:ETHUSDT") | test_enhancements.py |
| 16 | Webhook symbol prefix | test_enhancements.py |
| 17 | Invalid signal → 400 | test_enhancements.py |
| 18 | NEUTRAL signal rejected | test_enhancements.py |
| 19 | String ts/price parsing | test_enhancements.py |
| 20 | Unparseable ts → 400 | test_enhancements.py |
| 21 | Unparseable price → 400 | test_enhancements.py |
| 22 | Wrong secret → 401 | test_enhancements.py |

## Varsayımlar

1. **Redis erişilebilir**: Servis Redis olmadan çalışamaz.
2. **TradingView payload formatı**: Minimum `secret`, `indicator`, `symbol`, `tf`, `signal` alanları beklenir.
3. **Scheduler watchlist**: Default `["ETHUSDT", "BTCUSDT"]`. Admin `/config` ile değiştirilebilir.
4. **AI provider**: `dummy` modda template-based fallback. `openai` modda API çağrısı.
5. **Binance API**: `fapi.binance.com` public endpoint. API key gerekmez.
6. **Zaman**: Sunucu saati UTC olmalıdır.
7. **Execution yok**: Emir gönderme yoktur.
8. **LTRIM**: Default 1000 event/symbol. Config ile ayarlanabilir.

## Dosya Yapısı

```
├── app/
│   ├── main.py              # FastAPI app + lifespan + scheduler start/stop
│   ├── config.py             # pydantic-settings
│   ├── dependencies.py       # Shared dependencies
│   ├── routers/
│   │   ├── webhook.py        # POST /tv-webhook
│   │   ├── status.py         # GET /status
│   │   ├── latest.py         # GET /latest (two-layer)
│   │   ├── events.py         # GET /events (filterable)
│   │   └── admin.py          # POST /config
│   ├── schemas/
│   │   ├── webhook.py        # WebhookPayload, NormalizedEvent
│   │   ├── evaluation.py     # RulesOutput, LatestEvaluation, LatestRules, LatestAI
│   │   └── config.py         # RuntimeConfig (extended)
│   ├── modules/
│   │   ├── redis_client.py   # Async Redis lifecycle
│   │   ├── normalizer.py     # Payload normalization + TF mapping
│   │   ├── dedup.py          # Dedup + rate limit
│   │   ├── aggregator.py     # Event aggregation (optimised LRANGE)
│   │   ├── rules_engine.py   # Deterministic scoring
│   │   ├── market_data.py    # Binance klines client
│   │   ├── ai_client.py      # AI explainer interface
│   │   ├── scheduler.py      # Scheduled refresh loop
│   │   └── locks.py          # Redis distributed lock helpers
│   └── utils/
│       └── logging.py        # structlog configuration
├── tests/
│   ├── conftest.py           # FakeRedis + fixtures
│   ├── test_webhook.py       # Original 7 tests
│   └── test_enhancements.py  # 7 new tests
├── .env.example
├── requirements.txt
├── pytest.ini
├── run.py
├── sample_payloads.json
├── CHANGELOG.md
└── README.md
```
