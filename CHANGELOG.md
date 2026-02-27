# Changelog

## v1.2.0 – TradingView Indicator (Study) Uyumluluğu

**Temel gerçek:** BigBeluga, ChartPrime, SwiftAlgo script'leri **strategy değil indicator (study)**. `{{strategy.order.action}}` kullanılamaz; signal alanı her alert'te manuel set edilir.

### Güncellenen Dosyalar

| Dosya | Değişiklik |
|-------|-----------|
| `app/schemas/webhook.py` | `ts` ve `price` artık `str \| int \| float \| None` kabul eder (TradingView her şeyi string gönderir) |
| `app/modules/normalizer.py` | **Symbol prefix temizleme** (`BINANCE:ETHUSDT` → `ETHUSDT`, `.P` suffix kaldırma). **Signal strict validation** (sadece BUY/SELL kabul, NEUTRAL/CLOSE → 400). **ts/price string→number parsing** (parse edilemezse 400). `NormalizeError` sınıfı ile yapısal hata dönüşü |
| `app/routers/webhook.py` | Secret hatası artık **401** döner (eskiden 200 rejected). `NormalizeError` handle eder → 400. Symbol prefix normalize desteği |
| `tests/test_webhook.py` | Secret testi 401 beklemeye güncellendi |
| `tests/test_enhancements.py` | **8 yeni test** eklendi: symbol prefix normalize, webhook symbol prefix, invalid signal 400, NEUTRAL reject, string ts/price, unparseable ts/price, 401 secret |
| `sample_payloads.json` | Tamamen yeniden yazıldı: 18 alert şablonu (9 BUY + 9 SELL), TradingView placeholder kullanımı, actual payload örneği |
| `README.md` | TradingView bölümü güncellendi: indicator vs strategy açıklaması, 18 alert kurulumu, otomatik normalization listesi, 22 test senaryosu |

### Toplam Test: 22

---

## v1.1.0 – Production Enhancements

### Yeni Dosyalar
| Dosya | Açıklama |
|-------|----------|
| `app/modules/scheduler.py` | Scheduled refresh loop (rules 30s, AI 120s) – watchlist semboller için |
| `app/modules/locks.py` | Redis distributed lock (SET NX PX) + Lua compare-and-delete |
| `app/routers/events.py` | `GET /events` – filtrelenebilir event geçmişi endpoint'i |
| `tests/test_enhancements.py` | 7 yeni test: events, LTRIM, scheduler, lock, TF normalization |
| `CHANGELOG.md` | Bu dosya |

### Güncellenen Dosyalar

| Dosya | Değişiklik |
|-------|-----------|
| `app/schemas/config.py` | `RuntimeConfig`'e eklenen alanlar: `watchlist_symbols`, `refresh_rules_seconds`, `refresh_ai_seconds`, `events_max_per_symbol` |
| `app/schemas/evaluation.py` | Yeni modeller: `LatestRules`, `LatestAI`, `LatestEvaluation` (iki-katmanlı /latest payload). `Evaluation`'a `evaluation_id` + `generated_at` eklendi |
| `app/modules/normalizer.py` | TF normalization map eklendi ("15"→"15m", "60"→"1h", "240"→"4h", büyük/küçük harf desteği). Geçersiz TF → `None` döner (caller 400 verir). `normalize_tf()` public fonksiyonu eklendi |
| `app/modules/aggregator.py` | `aggregate()` fonksiyonuna `max_events` parametresi eklendi. LRANGE optimizasyonu: sadece son N event çekilir |
| `app/routers/webhook.py` | LTRIM eklendi (event yazımından sonra). AI single-flight lock entegrasyonu. Geçersiz TF → 400 response. İki-katmanlı `store_latest()` kullanımı |
| `app/routers/latest.py` | `LatestEvaluation` schema kullanımı (iki-katmanlı response) |
| `app/main.py` | Scheduler start/stop lifespan entegrasyonu. Events router eklendi |
| `tests/conftest.py` | FakeRedis'e `ltrim`, `eval` (Lua), `set(nx=, px=)` desteği eklendi. `AsyncMock` ile düzgün async patching |
| `README.md` | Scheduler, /events, LTRIM, lock stratejisi, TF normalization dokümantasyonu |

### Kritik Eksikler Tamamlandı

1. **Scheduled Refresh**: Alert gelmese bile watchlist semboller otomatik güncellenir
2. **Event Store LTRIM**: `tv:events:{symbol}` listesi max 1000 event ile sınırlı
3. **GET /events endpoint**: Panel için filtrelenebilir event geçmişi
4. **AI single-flight lock**: Aynı symbol için concurrent AI üretimi engellenir
5. **TF normalization**: "15", "60", "240", "1H", "4H" gibi varyasyonlar otomatik çevrilir
6. **İki-katmanlı /latest**: `latest_rules` (hızlı) + `latest_ai` (yavaş) ayrımı
