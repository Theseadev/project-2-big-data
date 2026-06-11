import streamlit as st
import pandas as pd
import plotly.express as px
import os
import pickle

st.set_page_config(page_title="Energy Analytics Dashboard - 230104040057", layout="wide")

# ==========================================
# OTOMATISASI ABSOLUTE PATH (WAJIB)
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

st.title("⚡ Smart Energy Consumption Analytics Dashboard")
st.markdown("### NIM: 230104040057 | Kelas: TI23B")

def load_parquet_data(folder_name):
    path = os.path.join(OUTPUT_DIR, folder_name)
    if os.path.exists(path):
        return pd.read_parquet(path)
    return pd.DataFrame()

df_total = load_parquet_data("energy_total")
df_time = load_parquet_data("energy_time")

if df_total.empty or df_time.empty:
    st.error("Data Parquet tidak ditemukan! Sila jalankan `main_uas_230104040057.py` terlebih dahulu di terminal.")
else:
    # Sidebar Filter
    st.sidebar.header("Filter Navigasi")
    sector_list = df_time['sector'].unique()
    selected_sector = st.sidebar.selectbox("Pilih Sektor Kawasan:", sector_list)

    # Filter data
    filtered_time = df_time[df_time['sector'] == selected_sector].copy()
    filtered_time['waktu_log'] = filtered_time.apply(lambda r: f"{int(r['hour'])}:{int(r['minute_10']):02d}", axis=1)
    filtered_time = filtered_time.sort_values(by=['hour', 'minute_10'])

    total_val = df_total[df_total['sector'] == selected_sector]['total_power'].values[0]

    # Metrics & Graphics Layout
    col1, col2 = st.columns([1, 3])
    with col1:
        st.metric(label=f"Total Konsumsi {selected_sector}", value=f"{total_val:,.2f} kWh")
        st.info("Data mencakup durasi pemantauan IoT 150 menit.")

    with col2:
        st.subheader(f"Tren Konsumsi Energi per 10 Menit ({selected_sector})")
        fig = px.line(filtered_time, x='waktu_log', y='power_per_10min', 
                      labels={'waktu_log': 'Waktu (Jam:Menit)', 'power_per_10min': 'Daya (kWh)'},
                      markers=True, template="plotly_dark")
        
        # PERBAIKAN DI SINI: Menggunakan width='stretch' sesuai regulasi versi Streamlit 2026
        st.plotly_chart(fig, width='stretch')

    st.markdown("---")

    # AI Section
    st.subheader("🤖 AI Forecasting Model (Linear Regression)")
    model_path = os.path.join(BASE_DIR, "linear_model.pkl")
    
    if os.path.exists(model_path):
        with open(model_path, 'rb') as f:
            model = pickle.load(f)

        input_hour = st.slider("Pilih Target Jam untuk Prediksi:", 0, 23, 15)
        prediction = model.predict([[input_hour]])[0]
        
        st.success(f"Estimasi Prediksi Rata-rata Konsumsi pada Jam **{input_hour}:00** adalah **{prediction:.2f} kWh**")