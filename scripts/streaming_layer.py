from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("StreamingPipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = """
user_id INT,
product STRING,
price DOUBLE,
city STRING,
timestamp STRING
"""

stream_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger",1) \
    .json("stream_data")

query = stream_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path","data/serving/stream") \
    .option("checkpointLocation","logs/stream_checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()