from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    while True:
        data = {
            "nama": random.choice(["Andi", "Budi", "Citra"]),
            "rekening": str(random.randint(100000, 999999)),
            "jumlah": random.randint(100000, 100000000),
            "lokasi": random.choice(["Jakarta", "Luar Negeri"])
        }

        producer.send("bank_topic", value=data)
        producer.flush()  # biar langsung terkirim
        print("Terkirim:", data)

        time.sleep(2)

except KeyboardInterrupt:
    print("\nDihentikan oleh user")

finally:
    producer.close()