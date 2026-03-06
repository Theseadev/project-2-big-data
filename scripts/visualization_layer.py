# =====================================
# VISUALIZATION LAYER
# =====================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
import pandas as pd
import matplotlib.pyplot as plt
import os

# ============================
# INIT SPARK
# ============================
spark = SparkSession.builder \
    .appName("VisualizationLayer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("========================================")
print("        VISUALIZATION LAYER STARTED     ")
print("========================================")

# ============================
# CREATE REPORT FOLDER
# ============================
if not os.path.exists("reports"):
    os.makedirs("reports")

# ============================
# LOAD CLEAN DATA (SILVER)
# ============================
print("Loading Clean Parquet Data...")

df = spark.read.parquet("data/clean/parquet/")

# ============================
# REVENUE PER CATEGORY
# ============================
print("Generating Revenue per Category Chart...")

category_df = (
    df.groupBy("category")
    .agg(_sum("total_amount").alias("total_revenue"))
)

# Convert to Pandas for visualization
category_pd = category_df.toPandas()

# Sort descending
category_pd = category_pd.sort_values("total_revenue", ascending=False)

# ============================
# CREATE BAR CHART
# ============================
plt.figure(figsize=(8, 5))

plt.bar(
    category_pd["category"],
    category_pd["total_revenue"]
)

plt.xticks(rotation=45)
plt.title("Revenue per Category")
plt.xlabel("Category")
plt.ylabel("Total Revenue")

plt.tight_layout()

# Save chart
plt.savefig("reports/category_revenue.png")

print("Visualization saved to reports/category_revenue.png")

# ============================
# STOP SPARK
# ============================
spark.stop()

print("========================================")
print("     VISUALIZATION LAYER COMPLETED      ")
print("========================================")