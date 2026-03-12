import streamlit as st
import pandas as pd
import os
import time

# Konfigurasi Halaman
st.set_page_config(
    page_title="Real-Time E-Commerce Dashboard",
    layout="wide"
)

st.title("Real-Time E-Commerce Analytics Dashboard")

DATA_PATH = "data/serving/stream"
placeholder = st.empty()

def load_stream_data():
    if not os.path.exists(DATA_PATH):
        return pd.DataFrame()
    
    files = [f for f in os.listdir(DATA_PATH) if f.endswith(".parquet")]
    
    if len(files) == 0:
        return pd.DataFrame()
    
    df = pd.concat(
        [pd.read_parquet(os.path.join(DATA_PATH, f)) for f in files],
        ignore_index=True
    )
    return df

# Loop Utama untuk Real-Time Update
while True:
    with placeholder.container():
        try:
            df = load_stream_data()

            if df.empty:
                st.info("Waiting for streaming data...")
                time.sleep(5)
                continue

            # Konversi timestamp
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])

            # ==============================
            # KPI METRICS
            # ==============================
            
            st.subheader("Key Metrics")
            
            coll, col2, col3, col4 = st.columns(4)
            
            total_transactions = len(df)
            total_revenue = df["price"].sum()
            avg_transaction = df["price"].mean()
            unique_cities = df["city"].nunique()

            coll.metric("Total Transactions", total_transactions)
            col2.metric("Total Revenue", f"${int(total_revenue)}")
            col3.metric("Avg Transaction", f"${int(avg_transaction)}")
            col4.metric("Cities", unique_cities)

            st.divider()

            # ==============================
            # CHART SECTION
            # ==============================

            colA, colB = st.columns(2)

            # Revenue per City
            with colA:
                st.subheader("Revenue by City")
                city_sales = (
                    df.groupby("city")["price"]
                    .sum()
                    .sort_values(ascending=False)
                )
                st.bar_chart(city_sales)

            # Top Products
            with colB:
                st.subheader("Top Products")
                product_sales = (
                    df.groupby("product")["price"]
                    .sum()
                    .sort_values(ascending=False)
                )
                st.bar_chart(product_sales)

            st.divider()

            # ==============================
            # REVENUE TREND
            # ==============================

            st.subheader("Revenue Trend")

            if "timestamp" in df.columns:
                revenue_trend = (
                    df.set_index("timestamp")
                    .resample("10s")["price"]
                    .sum()
                )
                st.line_chart(revenue_trend)

            st.divider()

            # ==============================
            # LIVE TRANSACTIONS TABLE
            # ==============================

            st.subheader("Live Transactions")

            st.dataframe(
                df.sort_values("timestamp", ascending=False).head(30),
                use_container_width=True
            )

        except Exception as e:
            st.error("Streaming data not ready")
            st.text(str(e))

    time.sleep(5)