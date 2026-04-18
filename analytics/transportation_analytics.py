import pandas as pd
import os

# ===========================
# LOAD DATA
# ===========================
def load_data(path):
    if not os.path.exists(path):
        return pd.DataFrame()

    files = [f for f in os.listdir(path) if f.endswith(".parquet")]

    if not files:
        return pd.DataFrame()

    df = pd.concat(
        [pd.read_parquet(os.path.join(path, f)) for f in files],
        ignore_index=True
    )

    return df

# ===========================
# PREPROCESS
# ===========================
def preprocess(df):
    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    return df

# ===========================
# METRICS
# ===========================
def compute_metrics(df):
    if df.empty:
        return {
            "total_trips": 0,
            "total_fare": 0,
            "top_location": "-"
        }

    return {
        "total_trips": len(df),
        "total_fare": df["fare"].sum(),
        "top_location": df.groupby("location")["fare"].sum().idxmax()
    }

# ===========================
# PEAK HOUR
# ===========================
def detect_peak_hour(df):
    if df.empty:
        return None

    df["hour"] = df["timestamp"].dt.hour
    return df.groupby("hour").size().idxmax()

# ===========================
# VISUALIZATION DATA
# ===========================
def fare_per_location(df):
    if df.empty:
        return pd.Series()

    return df.groupby("location")["fare"].sum().sort_values(ascending=False)

def vehicle_distribution(df):
    if df.empty:
        return pd.Series()

    return df.groupby("vehicle_type").size().sort_values(ascending=False)

def mobility_trend(df):
    if df.empty:
        return pd.Series()

    df = df.set_index("timestamp")
    return df["fare"].resample("10s").sum()

# ===========================
# NEW (PRAKTIKUM 6)
# WINDOW AGGREGATION
# ===========================
def traffic_per_window(df):
    """
    Agregasi jumlah trip per menit (windowing)
    Digunakan untuk visualisasi skala besar (efficient rendering)
    """
    if df.empty:
        return None

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df.set_index("timestamp") \
            .resample("1min") \
            .size()

# ===========================
# ANOMALY DETECTION
# ===========================
def detect_anomaly(df):
    if df.empty:
        return pd.DataFrame()

    # contoh: fare tinggi dianggap anomali
    return df[df["fare"] > 80000]