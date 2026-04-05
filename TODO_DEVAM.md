# Devam Edilecek İşler

## TAMAMLANDI

### signal_engine.py değişiklikleri: ✅
1. Sinyal motoru artık HER ZAMAN sinyal üretiyor (pozisyon açık olsa bile)
2. `on_price_tick`'ten pozisyon kontrolü kaldırıldı
3. `_check_divergence`'dan `used_a.add` kaldırıldı — sadece işleme girildiğinde ekleniyor
4. `last_signal` + `last_signal_bar` alanları eklendi
5. `try_execute_signal()` fonksiyonu eklendi — pozisyon yoksa True döner, used_a ekler
6. Pozisyon kapandığında (`check_position_closed` + `order_stream`) last_signal kontrol ediliyor, geçerliyse hemen işleme giriliyor
7. Sinyal her zaman DB'ye loglanıyor — `entered=True/False` ve `skip_reason` ile

### order_stream.py değişiklikleri: ✅
- Pozisyon kapandığında last_signal geçerliyse hemen execute_trade çağırılıyor

### Adım 7: chart-data simülasyon kaldırma (status.py) ✅
- `/api/chart-data` endpoint'indeki simülasyon signals ve trades hesaplaması kaldırıldı
- Sadece candles + RSI + position dönüyor
- Simülasyon ✓TP/✗SL satırları kaldırıldı
- Simülasyon ▼SELL/▲BUY sinyal satırları kaldırıldı

### Adım 8: Monitor gerçek sinyal gösterimi (monitor.html) ✅
- Mum tablosundaki sinyal satırları `signal_log` DB'den geliyor (`/api/st-signals`)
- Her sinyal: yön, fiyat, RSI A→B, gap, skip_reason
- Binance giriş/çıkış satırları KORUNDU (değişmedi)
- `entered=True` ise yeşil/kırmızı sinyal badge
- `entered=False` ise gri badge + skip_reason gösteriliyor
- `/api/st-signals` endpoint'i artık tüm sinyalleri döndürüyor (entered_only=false varsayılan)
- İstatistik kartı DB sinyallerinden hesaplanıyor
