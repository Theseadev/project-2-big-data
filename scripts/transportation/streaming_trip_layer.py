from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("TransportationStreaming").getOrCreate()

schema = StructType([
    StructField("trip_id", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("location", StringType()),
    StructField("distance", DoubleType()),
    StructField("fare", DoubleType()),
    StructField("timestamp", StringType())
])

df = spark.readStream.schema(schema).json("stream_data/transportation")

df = df.withColumn("timestamp", to_timestamp("timestamp"))

df.writeStream \
    .format("parquet") \
    .option("path", "data/serving/transportation") \
    .option("checkpointLocation", "data/checkpoints/transportation") \
    .start() \
    .awaitTermination()