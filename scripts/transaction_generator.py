import json
import random
import time
import os
from datetime import datetime

output_folder = "stream_data"

products = ["Laptop","Mouse","Keyboard","Monitor","Headset","Webcam"]
cities = ["Jakarta","Bandung","Surabaya","Medan","Yogyakarta"]

counter = 1

while True:

    transaction = {
        "user_id": random.randint(100,200),
        "product": random.choice(products),
        "price": random.randint(50,2000),
        "city": random.choice(cities),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    filename = f"transaction_{counter}.json"
    filepath = os.path.join(output_folder,filename)

    with open(filepath,"w") as f:
        json.dump(transaction,f)

    print("Generated:",transaction)

    counter += 1

    time.sleep(3)