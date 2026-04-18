import streamlit as st
import time
import sys
import os

# ===========================
# FIX MODULE PATH (WAJIB)
# ===========================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# ===========================
# IMPORT MODULE
# ===========================
from analytics import transportation_analytics as ta
from alerts import transportation_alert as alert

# ===========================
# CONFIG
# ===========================
DATA_PATH = "data/serving/transportation"

st.set_page_config(
    page_title="Smart Transportation Dashboard",
    layout="wide"
)

st.title("🚗 Smart Transportation - Real-Time Analytics (Big Data Optimized)")

# ===========================
# AUTO REFRESH
# ===========================
REFRESH_INTERVAL = 5
placeholder = st.empty()

# ===========================
# MAIN LOOP
# ===========================
while True:
    with placeholder.container():

        # ===========================
        # LOAD DATA
        # ===========================
        df = ta.load_data(DATA_PATH)

        if df.empty:
            st.warning("⏳ Waiting for streaming transportation data...")
            time.sleep(REFRESH_INTERVAL)
            continue

        # ===========================
        # PREPROCESS
        # ===========================
        df = ta.preprocess(df)

        # ===========================
        # 🔥 OPTIMASI BIG DATA
        # ===========================
        # Ambil subset data untuk visualisasi agar tidak berat
        df_sample = df.tail(1000)

        # ===========================
        # METRICS
        # ===========================
        try:
            metrics = ta.compute_metrics(df)
        except Exception as e:
            st.error(f"Error computing metrics: {e}")
            time.sleep(REFRESH_INTERVAL)
            continue

        coll, col2, col3 = st.columns(3)

        coll.metric("Total Trips", metrics["total_trips"])
        col2.metric("Total Fare", int(metrics["total_fare"]))
        col3.metric("Top Location", metrics["top_location"])

        st.divider()

        # ===========================
        # PEAK HOUR
        # ===========================
        try:
            peak_hour = ta.detect_peak_hour(df)
            st.info(f"🚦 Peak traffic hour: {peak_hour}:00")
        except Exception:
            st.warning("Tidak dapat menghitung peak hour")

        # ===========================
        # ALERTS
        # ===========================
        try:
            alerts = alert.generate_alert(df)
            if alerts:
                st.subheader("🚨 Traffic Alerts")
                for a in alerts:
                    st.error(a)
        except Exception as e:
            st.warning(f"Alert error: {e}")

        st.divider()

        # ===========================
        # 🔥 VISUALISASI SKALA BESAR
        # ===========================
        try:
            col1, col2 = st.columns(2)

            # ---------------------------
            # 1. TRAFFIC WINDOW (NEW - PRAKTIKUM 6)
            # ---------------------------
            st.subheader("📊 Real-Time Traffic (Window Aggregation)")

            traffic_window = ta.traffic_per_window(df)

            if traffic_window is not None:
                st.line_chart(traffic_window)

            # ---------------------------
            # 2. FARE PER LOCATION
            # ---------------------------
            with col1:
                st.subheader("🚕 Fare per Location")
                st.bar_chart(ta.fare_per_location(df_sample))

            # ---------------------------
            # 3. VEHICLE DISTRIBUTION   
            # ---------------------------
            with col2:
                st.subheader("🚙 Vehicle Distribution")
                st.bar_chart(ta.vehicle_distribution(df_sample))

            # ---------------------------
            # 4. MOBILITY TREND (DOWNSAMPLED)
            # ---------------------------
            st.subheader("📈 Mobility Trend (Optimized)")
            st.line_chart(ta.mobility_trend(df_sample))

        except Exception as e:
            st.warning(f"Visualization error: {e}")

        st.divider()

        # ===========================
        # ANOMALY
        # ===========================
        try:
            st.subheader("⚠️ Abnormal Trips")

            anomaly_df = ta.detect_anomaly(df_sample)

            if not anomaly_df.empty:
                st.dataframe(anomaly_df.tail(20))
            else:
                st.success("No anomalies detected")

        except Exception as e:
            st.warning(f"Anomaly error: {e}")

        st.divider()

        # ===========================
        # 🔥 LIVE DATA (LIMITED)
        # ===========================
        st.subheader("📋 Live Trip Data (Limited View)")
        st.dataframe(df_sample.tail(50))

    time.sleep(REFRESH_INTERVAL)