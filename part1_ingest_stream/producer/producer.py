"""
Steam Reviews Kafka Producer
─────────────────────────────
steam_reviews.csv dosyasını okur ve her satırı JSON formatında
'steam-reviews' Kafka topic'ine yayınlar.

Özellikler:
  - Chunk bazlı okuma (düşük bellek kullanımı)
  - Ayarlanabilir gönderim hızı (MESSAGES_PER_SECOND env)
  - Retry logic (Kafka bağlantı hatası için)
  - Hatalı satır atlama ve loglama
  - Özet istatistik raporlama
"""

import os
import json
import time
import hashlib
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

# ─── Konfigürasyon (environment variable ile ayarlanabilir) ───────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "steam-reviews")
CSV_PATH = os.getenv("CSV_PATH", "/data/steam_reviews.csv")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1000"))  # pandas chunksize

# Gönderim hızı ayarı
MESSAGES_PER_SECOND = int(os.getenv("MESSAGES_PER_SECOND", "50"))
# Toplam göndereceğimiz mesaj limiti (0 veya negatif = sınırsız)
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "0"))
# Delay sınırları (saniye)
MIN_DELAY = 0.001
MAX_DELAY = 1.0

# Retry ayarları
MAX_RETRIES = 3
RETRY_DELAY = 5  # saniye


def calculate_delay(messages_per_second: int) -> float:
    """Mesaj başına bekleme süresini hesapla ve sınırlar içinde tut."""
    if messages_per_second <= 0:
        return MAX_DELAY
    delay = 1.0 / messages_per_second
    return max(MIN_DELAY, min(MAX_DELAY, delay))


def create_producer() -> KafkaProducer:
    """
    Kafka Producer oluştur.
    Bağlantı hatası durumunda MAX_RETRIES kadar yeniden dene.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"[INFO] Kafka bağlantısı başarılı (deneme {attempt}/{MAX_RETRIES})")
            return producer
        except NoBrokersAvailable:
            print(
                f"[WARN] Kafka broker bulunamadı, {RETRY_DELAY}s bekleniyor… "
                f"(deneme {attempt}/{MAX_RETRIES})"
            )
            time.sleep(RETRY_DELAY)
    raise RuntimeError(
        f"[ERROR] {MAX_RETRIES} deneme sonrası Kafka broker'a bağlanılamadı."
    )


def transform_row(row: pd.Series) -> dict | None:
    """
    CSV satırını Kafka mesaj formatına dönüştür.
    Eksik veya hatalı satırlar için None döner.
    """
    try:
        # Zorunlu alanların kontrolü
        required_fields = ["app_id", "app_name", "review_text", "review_score"]
        for field in required_fields:
            if field not in row.index or pd.isna(row[field]):
                raise ValueError(f"Eksik alan: {field}")

        # timestamp dönüşümü — Unix timestamp varsa ISO format'a çevir, yoksa şimdiki zaman
        if "timestamp_created" in row.index and pd.notna(row["timestamp_created"]):
            ts_raw = row["timestamp_created"]
            try:
                ts_value = int(float(ts_raw))
                iso_timestamp = datetime.fromtimestamp(ts_value, tz=timezone.utc).isoformat()
            except (ValueError, TypeError, OSError):
                iso_timestamp = str(ts_raw)
        else:
            iso_timestamp = datetime.now(tz=timezone.utc).isoformat()

        # review_score: 1 (positive) veya -1 (negative) olarak normalize et
        score = int(row["review_score"])
        if score not in (1, -1):
            # Farklı encoding'ler için: 0 → -1, 1 → 1
            score = 1 if score > 0 else -1

        # review_votes: eksikse 0 kabul et
        review_votes = int(row["review_votes"]) if pd.notna(row.get("review_votes")) else 0

        # review_text: maksimum 500 karakter
        review_text = str(row["review_text"])[:500]

        # user_id: Steam datasetinde author kolonu olmadığı için,
        # (app_id + review_text) hash'inden deterministik bir kullanıcı ID türetiyoruz.
        # Bu sayede aynı yorum tekrar işlense bile aynı user_id elde edilir (idempotent).
        user_seed = f"{int(row['app_id'])}|{review_text}".encode("utf-8")
        user_id = "u_" + hashlib.md5(user_seed).hexdigest()[:12]

        message = {
            "timestamp": iso_timestamp,
            "user_id": user_id,
            "event_type": "steam_review",
            "app_id": int(row["app_id"]),
            "app_name": str(row["app_name"]),
            "review_text": review_text,
            "review_score": score,
            "review_votes": review_votes,
        }
        return message

    except (ValueError, TypeError, KeyError) as e:
        return None


def main():
    """Ana producer döngüsü."""
    print("=" * 60)
    print("  Steam Reviews Kafka Producer")
    print("=" * 60)
    print(f"[CONFIG] Kafka Broker  : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[CONFIG] Topic         : {KAFKA_TOPIC}")
    print(f"[CONFIG] CSV Dosyası   : {CSV_PATH}")
    print(f"[CONFIG] Chunk Size    : {CHUNK_SIZE}")
    print(f"[CONFIG] Mesaj Hızı    : {MESSAGES_PER_SECOND} msg/s")
    print(f"[CONFIG] Max Mesaj     : {MAX_MESSAGES if MAX_MESSAGES > 0 else 'sınırsız'}")
    print("=" * 60)

    # Kafka Producer oluştur
    producer = create_producer()

    # Gönderim hızı hesapla
    delay = calculate_delay(MESSAGES_PER_SECOND)
    print(f"[INFO] Mesaj arası bekleme: {delay:.4f}s")

    # İstatistik sayaçları
    total_sent = 0
    total_failed = 0
    positive_count = 0
    negative_count = 0
    start_time = time.time()
    last_log_time = start_time
    last_app_name = ""

    # CSV dosyasını chunk chunk oku
    try:
        reader = pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, low_memory=False)
    except FileNotFoundError:
        print(f"[ERROR] CSV dosyası bulunamadı: {CSV_PATH}")
        producer.close()
        return

    for chunk in reader:
        for idx, row in chunk.iterrows():
            # Satırı dönüştür
            message = transform_row(row)

            if message is None:
                # Hatalı/eksik satırı logla ve atla
                total_failed += 1
                if total_failed <= 10:
                    print(f"[WARN] Satır {idx} atlandı (eksik/hatalı veri)")
                elif total_failed == 11:
                    print("[WARN] Daha fazla hatalı satır loglanmayacak…")
                continue

            # Kafka'ya gönder
            try:
                producer.send(KAFKA_TOPIC, value=message)
                total_sent += 1
                last_app_name = message["app_name"]

                # Positive/Negative sayacı güncelle
                if message["review_score"] == 1:
                    positive_count += 1
                else:
                    negative_count += 1

            except KafkaError as e:
                total_failed += 1
                print(f"[ERROR] Kafka gönderim hatası (satır {idx}): {e}")
                continue

            # Her 100 mesajda bir loglama
            if total_sent % 100 == 0:
                current_time = time.time()
                elapsed = current_time - last_log_time
                speed = 100 / elapsed if elapsed > 0 else 0
                last_log_time = current_time
                print(
                    f"[INFO] Gönderilen mesaj sayısı: {total_sent} | "
                    f"Son oyun: {last_app_name} | "
                    f"Hız: {speed:.1f} msg/s"
                )

            # Hız kontrolü
            time.sleep(delay)

            # MAX_MESSAGES limitine ulaştıysak çık
            if MAX_MESSAGES > 0 and total_sent >= MAX_MESSAGES:
                print(f"[INFO] MAX_MESSAGES limitine ulaşıldı ({MAX_MESSAGES:,}), durduruluyor.")
                producer.flush()
                break
        else:
            # iç döngü break yapmadıysa chunk sonunda flush
            producer.flush()
            continue
        # iç döngüde break olduysa dış döngüden de çık
        break

    # ─── Özet İstatistikler ───────────────────────────────────────────────────
    end_time = time.time()
    total_elapsed = end_time - start_time
    avg_speed = total_sent / total_elapsed if total_elapsed > 0 else 0
    pos_ratio = (positive_count / total_sent * 100) if total_sent > 0 else 0
    neg_ratio = (negative_count / total_sent * 100) if total_sent > 0 else 0

    print("\n" + "=" * 60)
    print("  ÖZET İSTATİSTİKLER")
    print("=" * 60)
    print(f"  Toplam gönderilen mesaj  : {total_sent:,}")
    print(f"  Başarısız mesaj sayısı   : {total_failed:,}")
    print(f"  Toplam süre              : {total_elapsed:.1f}s")
    print(f"  Ortalama gönderim hızı   : {avg_speed:.1f} msg/s")
    print(f"  Positive review oranı    : {pos_ratio:.1f}% ({positive_count:,})")
    print(f"  Negative review oranı    : {neg_ratio:.1f}% ({negative_count:,})")
    print("=" * 60)

    # Producer'ı kapat
    producer.close()
    print("[INFO] Producer kapatıldı. İşlem tamamlandı.")


if __name__ == "__main__":
    main()
