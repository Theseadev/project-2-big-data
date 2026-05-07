from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as _sum, hour
import random
from datetime import datetime, timedelta
import os
import shutil

# --- Sesi 1: Setup Path (Disesuaikan dengan struktur folder user) ---
# Karena file ini di /scripts, kita simpan 'output' di folder utama proyek
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Mengarahkan output ke: bigdata-project/output (naik 1 level dari folder scripts)
PROJECT_ROOT = os.path.dirname(BASE_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

# --- Sesi 2: Init Spark ---
spark = SparkSession.builder \
    .appName("UTS_BigData_Processing") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("🚀 Spark Ready... Memulai Pemrosesan")

# --- Sesi 3: Clean Output Folder ---
# Membersihkan folder lama agar data tidak konflik[cite: 1]
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Sesi 4: Generate Dummy Data ---
# Simulasi data sensor selama 100 menit untuk 3 area[cite: 1]
locations = ["AreaA", "AreaB", "AreaC"]
start_time = datetime(2026, 1, 1, 7, 0)
sensor_data = []

for i in range(100):
    for loc in locations:
        sensor_data.append((
            start_time + timedelta(minutes=i),
            loc,
            random.randint(10, 100)
        ))

# Mengubah daftar menjadi DataFrame Spark[cite: 1]
sensor_df = spark.createDataFrame(sensor_data, ["timestamp", "location", "vehicle_count"])

# --- Sesi 5: Processing Logic ---
# 1. Menghitung total kendaraan per lokasi[cite: 1]
traffic_df = sensor_df.groupBy("location").agg(_sum("vehicle_count").alias("total_vehicle"))

# 2. Mengelompokkan data per 10 menit (Time Series)[cite: 1]
traffic_time_df = sensor_df.groupBy(
    window(col("timestamp"), "10 minutes"), "location"
).agg(_sum("vehicle_count").alias("total_vehicle"))

# 3. Menyiapkan fitur 'hour' untuk keperluan Machine Learning[cite: 1]
ml_df = sensor_df.withColumn("hour", hour(col("timestamp")))

# --- Sesi 6 & 7: Save to Parquet and Stop ---
def save_data(df, folder_name):
    try:
        path = os.path.join(OUTPUT_DIR, folder_name)
        # Menggunakan format Parquet untuk efisiensi penyimpanan[cite: 1]
        df.write.mode("overwrite").parquet(path)
        print(f"✅ Data {folder_name} berhasil disimpan di: {path}")
    except Exception as e:
        print(f"❌ Error saat menyimpan {folder_name}: {str(e)}")

save_data(traffic_df, "traffic")
save_data(traffic_time_df, "traffic_time")
save_data(ml_df, "ml_data")

# Mematikan Spark Session agar memori kembali lega[cite: 1]
spark.stop()
print("\n🏁 Pemrosesan Selesai. Sekarang kamu bisa menjalankan dashboard di folder /dashboard.")