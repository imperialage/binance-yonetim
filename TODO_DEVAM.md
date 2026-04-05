# Devam Edilecek İşler

## HAZIR — Deploy Bekliyor (commit edilmedi)

### signal_engine.py değişiklikleri:
1. Sinyal motoru artık HER ZAMAN sinyal üretiyor (pozisyon açık olsa bile)
2. `on_price_tick`'ten pozisyon kontrolü kaldırıldı
3. `_check_divergence`'dan `used_a.add` kaldırıldı — sadece işleme girildiğinde ekleniyor
4. `last_signal` + `last_signal_bar` alanları eklendi
5. `try_execute_signal()` fonksiyonu eklendi — pozisyon yoksa True döner, used_a ekler
6. Pozisyon kapandığında (`check_position_closed` + `order_stream`) last_signal kontrol ediliyor, geçerliyse hemen işleme giriliyor
7. Sinyal her zaman DB'ye loglanıyor — `entered=True/False` ve `skip_reason` ile

### order_stream.py değişiklikleri:
- Pozisyon kapandığında last_signal geçerliyse hemen execute_trade çağırılıyor

## YAPILMADI — Yeni Konuşmada Yapılacak

### Adım 7: chart-data simülasyon kaldırma (status.py)
- `/api/chart-data` endpoint'indeki simülasyon signals ve trades hesaplaması kaldırılacak
- Sadece candles + RSI dönecek
- Simülasyon ✓TP/✗SL satırları kaldırılacak
- Simülasyon ▼SELL/▲BUY sinyal satırları kaldırılacak

### Adım 8: Monitor gerçek sinyal gösterimi (monitor.html)
- Mum tablosundaki sinyal satırları `signal_log` DB'den gelecek (`/api/st-signals`)
- Her sinyal: yön, fiyat, RSI A→B, gap, skip_reason
- Binance giriş/çıkış satırları KALACAK (değişmeyecek)
- `entered=True` ise yeşil/kırmızı sinyal badge
- `entered=False` ise gri badge + skip_reason gösterilecek
