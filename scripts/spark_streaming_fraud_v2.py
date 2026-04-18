from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat, lit, substring, when, base64
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Init Spark
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .getOrCreate()

# =========================
# READ DATA DARI KAFKA
# =========================
df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank_topic") \
    .option("startingOffsets", "latest") \
    .load()

# =========================
# SCHEMA JSON
# =========================
schema = StructType([
    StructField("nama", StringType(), True),
    StructField("rekening", StringType(), True),
    StructField("jumlah", IntegerType(), True),
    StructField("lokasi", StringType(), True)
])

# =========================
# PARSING JSON
# =========================
df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# =========================
# CLEANING (optional tapi aman)
# =========================
df = df.dropna()

# =========================
# MASKING REKENING (FIX ERROR)
# =========================
df = df.withColumn(
    "rekening_masked",
    concat(lit("****"), substring(col("rekening"), -2, 2))
)

# =========================
# FRAUD DETECTION
# =========================
df = df.withColumn(
    "status",
    when(col("jumlah") > 50000000, "FRAUD")
    .when(col("lokasi") == "Luar Negeri", "FRAUD")
    .otherwise("NORMAL")
)

# =========================
# ENCRYPTION SEDERHANA
# =========================
df = df.withColumn(
    "jumlah_encrypted",
    base64(col("jumlah").cast("string"))
)

# =========================
# DEBUG (BIAR AMAN)
# =========================
df.printSchema()

# =========================
# OUTPUT KE PARQUET
# =========================
query = df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "stream_data/realtime_output/") \
    .option("checkpointLocation", "data/checkpoints/") \
    .start()

query.awaitTermination()