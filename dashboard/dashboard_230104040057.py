import streamlit as st
from pyspark.sql import SparkSession
import plotly.express as px
import pandas as pd
from sklearn.linear_model import LinearRegression
import os

# --- Sesi 1 & 2: Config & Path (Perbaikan Jalur) ---
# Karena file ini ada di /dashboard, kita naik satu level untuk menemukan folder /output
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

st.set_page_config(page_title="Traffic Dashboard", layout="wide")
st.title("🚦 Smart City AI Traffic Dashboard")

# --- Sesi 3: Init Spark Session ---
@st.cache_resource
def get_spark():
    # Inisialisasi Spark Session untuk membaca data Parquet[cite: 1]
    return SparkSession.builder.appName("Dashboard_App").getOrCreate()

spark = get_spark()

# --- Sesi 4: Data Loading ---
def load_parquet(folder_name):
    path = os.path.join(OUTPUT_DIR, folder_name)
    # Validasi keberadaan folder sebelum membaca untuk menghindari error[cite: 1]
    if not os.path.exists(path):
        st.error(f"❌ Folder {folder_name} tidak ditemukan di {path}! Jalankan 'scripts/main_uts_NIM.py' dulu.")
        st.stop()
    return spark.read.parquet(path).toPandas()

try:
    # Memuat 3 jenis data hasil olahan Big Data[cite: 1]
    pdf = load_parquet("traffic")
    pdf_time = load_parquet("traffic_time")
    pdf_ml = load_parquet("ml_data")
except Exception as e:
    st.error(f"Gagal memuat data: {e}")
    st.stop()

# --- Sesi 5: Sidebar Filter ---
locations = pdf["location"].unique()
selected_loc = st.sidebar.selectbox("Pilih Lokasi Analisis", locations)
filtered_pdf = pdf[pdf["location"] == selected_loc]

# --- Sesi 6: KPI Metrics ---
st.subheader("Key Performance Indicators")
col1, col2 = st.columns(2)
with col1:
    st.metric("Total Kendaraan (Semua Area)", int(pdf["total_vehicle"].sum()))
with col2:
    st.metric(f"Total di {selected_loc}", int(filtered_pdf["total_vehicle"].sum()))

# --- Sesi 7: Visualisasi Grafik ---
st.markdown("---")
c1, c2 = st.columns(2)

with c1:
    st.subheader("📈 Traffic Time Series")
    # Konversi format window Spark ke timestamp yang didukung Plotly[cite: 1]
    pdf_time["start_time"] = pdf_time["window"].apply(lambda x: x[0] if isinstance(x, tuple) else x.start)
    fig_line = px.line(pdf_time, x="start_time", y="total_vehicle", color="location", markers=True)
    st.plotly_chart(fig_line, use_container_width=True)

# --- Sesi 8: AI Prediction ---
with c2:
    st.subheader("🤖 AI Prediction (Linear Regression)")
    # Menyiapkan variabel X (fitur jam) dan y (target jumlah kendaraan)[cite: 1]
    X = pdf_ml[["hour"]]
    y = pdf_ml["vehicle_count"]
    
    # Melatih model Machine Learning sederhana (Linear Regression)[cite: 1]
    model = LinearRegression()
    model.fit(X, y)
    
    # Input jam interaktif menggunakan slider Streamlit[cite: 1]
    hour_input = st.slider("Prediksi Jam Ke-", 0, 23, 12)
    pred = model.predict([[hour_input]])
    
    st.success(f"Prediksi jumlah kendaraan pada jam {hour_input}:00 adalah **{max(0, int(pred[0]))}**")