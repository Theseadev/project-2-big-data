import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.linear_model import LinearRegression
import streamlit as st
import plotly.express as px

# 1. KONFIGURASI PATH ABSOLUT
BASE_PATH = "/home/fahrul/bigdata-project/uts-tbg-230104040057" 
OUTPUT_PATH = os.path.join(BASE_PATH, "output")

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("SmartCampusAttendance") \
    .getOrCreate()

# --- STAGE 1: GENERATE DATA ---
def generate_data():
    buildings = ["Fakultas Sains dan Teknologi", "Perpustakaan", "Auditorium"]
    data = []
    start_time = datetime.now()
    
    for i in range(100):
        current_time = start_time + timedelta(minutes=i)
        for bld in buildings:
            count = np.random.randint(20, 301)
            data.append((current_time, bld, count))
            
    columns = ["timestamp", "building", "attendance_count"]
    return spark.createDataFrame(data, columns)

df = generate_data()

# --- STAGE 2: SPARK TRANSFORMATION ---
df_total = df.groupBy("building").agg(F.sum("attendance_count").alias("total_attendance"))

df_time = df.groupBy(F.window("timestamp", "20 minutes"), "building") \
    .agg(F.avg("attendance_count").alias("avg_attendance")) \
    .withColumn("time_label", F.col("window.start"))

df_ml = df.withColumn("hour", F.hour("timestamp")) \
    .groupBy("hour", "building") \
    .agg(F.avg("attendance_count").alias("attendance_count"))

# --- STAGE 3: SAVE TO PARQUET ---
df_total.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/attendance_total")
df_time.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/attendance_time")
df_ml.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/ml_attendance")

# --- STAGE 4 & 5: DASHBOARD & MACHINE LEARNING ---
def run_dashboard():
    st.set_page_config(page_title="Smart Campus Dashboard", layout="wide")
    st.title("📊 Smart Campus Attendance Analytics")
    
    selected_bld = st.sidebar.selectbox("Pilih Gedung", 
                                        ["Fakultas Sains dan Teknologi", "Perpustakaan", "Auditorium"])
    
    # Load Data Parquet
    pd_total = pd.read_parquet(f"{OUTPUT_PATH}/attendance_total")
    pd_time = pd.read_parquet(f"{OUTPUT_PATH}/attendance_time")
    pd_ml = pd.read_parquet(f"{OUTPUT_PATH}/ml_attendance")
    
    # KPI
    total_val = pd_total[pd_total['building'] == selected_bld]['total_attendance'].values[0]
    st.metric(f"Total Kehadiran - {selected_bld}", f"{int(total_val)} Mahasiswa")
    
    # Grafik Plotly
    bld_time_data = pd_time[pd_time['building'] == selected_bld].sort_values('time_label')
    fig_line = px.line(bld_time_data, x='time_label', y='avg_attendance', 
                       title=f"Tren Kehadiran (Interval 20 Menit)",
                       markers=True)
    st.plotly_chart(fig_line, use_container_width=True)
    
    # Machine Learning
    st.subheader("🤖 AI Prediction: Kepadatan Kampus")
    bld_ml_data = pd_ml[pd_ml['building'] == selected_bld]
    X = bld_ml_data[['hour']].values
    y = bld_ml_data['attendance_count'].values
    
    model = LinearRegression()
    model.fit(X, y)
    
    current_hour = datetime.now().hour
    next_hour = [[(current_hour + 1) % 24]]
    prediction = model.predict(next_hour)
    
    st.info(f"Prediksi jumlah mahasiswa di **{selected_bld}** jam **{next_hour[0][0]}:00** adalah **{int(prediction[0])} orang**.")

if __name__ == "__main__":
    run_dashboard()