import os
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.linear_model import LinearRegression
import pickle

# ==========================================
# OTOMATISASI ABSOLUTE PATH (WAJIB)
# ==========================================
# Ini akan otomatis mengambil path absolut dari folder uas-tbg-230104040057 di WSL
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

print(f"📌 Working Directory (Absolute Path): {BASE_DIR}")

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("SmartEnergyAnalytics_230104040057") \
    .master("local[*]") \
    .getOrCreate()

# ==========================================
# 1. GENERATE DUMMY DATA (150 Menit)
# ==========================================
print("==> Generating Dummy Data untuk NIM Ganjil...")
sectors = ["Industrial_A", "Industrial_B", "Residential_C"]
start_time = datetime.now() - timedelta(minutes=150)

raw_data = []
for i in range(150):
    current_timestamp = start_time + timedelta(minutes=i)
    for sector in sectors:
        power_usage = float(random.randint(100, 1000))
        raw_data.append((current_timestamp, sector, power_usage))

schema = ["timestamp", "sector", "power_usage"]
df = spark.createDataFrame(raw_data, schema=schema)

# ==========================================
# 2. SPARK PROCESSING & AGREGASI
# ==========================================
print("==> Processing Data dengan PySpark...")

df_processed = df.withColumn("hour", F.hour("timestamp")) \
                 .withColumn("minute_10", F.floor(F.minute("timestamp") / 10) * 10)

# Agregasi 1: Total konsumsi energi per sektor
df_energy_total = df_processed.groupBy("sector") \
    .agg(F.sum("power_usage").alias("total_power"))

# Agregasi 2: Agregasi konsumsi tiap 10 menit
df_energy_time = df_processed.groupBy("sector", "hour", "minute_10") \
    .agg(F.sum("power_usage").alias("power_per_10min")) \
    .orderBy("hour", "minute_10")

# Agregasi 3: Dataset AI berdasarkan hour
df_ml_energy = df_processed.groupBy("hour") \
    .agg(F.avg("power_usage").alias("avg_power"))

# ==========================================
# 3. SIMPAN KE PARQUET (MODE OVERWRITE)
# ==========================================
print("==> Saving to Parquet...")
df_energy_total.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "energy_total"))
df_energy_time.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "energy_time"))
df_ml_energy.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "ml_energy"))

print("✔ File Parquet sukses dibuat menggunakan Absolute Path!")

# ==========================================
# 5. AI PREDICTION (Linear Regression)
# ==========================================
print("==> Training AI Model...")
ml_data = df_ml_energy.toPandas()

X = ml_data[['hour']].values
y = ml_data['avg_power'].values

model = LinearRegression()
model.fit(X, y)

# Simpan model machine learning ke dalam folder projek
model_path = os.path.join(BASE_DIR, "linear_model.pkl")
with open(model_path, 'wb') as f:
    pickle.dump(model, f)

print("✔ Model AI Linear Regression berhasil disimpan!")
spark.stop()