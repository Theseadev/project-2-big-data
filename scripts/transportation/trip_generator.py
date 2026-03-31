import json, time, random, os
from datetime import datetime

OUTPUT_PATH = "stream_data/transportation"
os.makedirs(OUTPUT_PATH, exist_ok=True)

locations = ["Jakarta", "Bandung", "Surabaya"]
vehicles = ["Car", "Motorbike", "Taxi"]

i = 1
while True:
    data = {
        "trip_id": f"TRX{i}",
        "vehicle_type": random.choice(vehicles),
        "location": random.choice(locations),
        "distance": random.uniform(1, 20),
        "fare": random.randint(10000, 100000),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    with open(f"{OUTPUT_PATH}/trip_{i}.json", "w") as f:
        json.dump(data, f)

    print("Generated Trip:", data)
    i += 1
    time.sleep(3)