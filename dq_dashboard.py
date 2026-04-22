import pandas as pd
import streamlit as st
from datetime import datetime, timezone

st.set_page_config(page_title="Payment Data Quality Dashboard", layout="wide")

st.title("📊 Payment Data Quality Dashboard")

# Load data
df = pd.read_csv("data/output/payment_events.csv")

# Convert timestamp
df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce")

# -------------------------
# Sidebar filter
# -------------------------
st.sidebar.header("Filters")

sources = st.sidebar.multiselect(
    "Source System",
    options=df["source_system"].dropna().unique(),
    default=df["source_system"].dropna().unique()
)

df = df[df["source_system"].isin(sources)]

# -------------------------
# 1. Schema Compliance
# -------------------------
st.header("✅ Schema Compliance Rate")

total_cells = df.shape[0] * df.shape[1]
null_cells = df.isnull().sum().sum()

compliance_rate = 100 * (1 - (null_cells / total_cells)) if total_cells else 0

st.metric("Compliance Rate (%)", f"{compliance_rate:.2f}")

st.caption("Based on null-value density across dataset")

# -------------------------
# 2. Data Freshness
# -------------------------
st.header("⏱️ Data Freshness")

now = datetime.now(timezone.utc)

freshness = (
    df.groupby("source_system")["event_timestamp"]
    .max()
    .reset_index()
)

freshness["minutes_since_last_event"] = freshness["event_timestamp"].apply(
    lambda x: (now - x).total_seconds() / 60 if pd.notnull(x) else None
)

st.dataframe(freshness)

# -------------------------
# 3. Transaction Volume Trend
# -------------------------
st.header("📉 Transaction Volume Trend")

df["date"] = df["event_timestamp"].dt.date

volume = df.groupby(["date", "source_system"]).size().reset_index(name="count")

st.line_chart(volume, x="date", y="count")

# -------------------------
# 4. Simple Anomaly Detection
# -------------------------
st.header("🚨 Anomaly Detection (Volume Drop)")

daily_volume = df.groupby("date").size()

mean = daily_volume.mean()
std = daily_volume.std()

threshold = mean - 2 * std

anomalies = daily_volume[daily_volume < threshold]

if not anomalies.empty:
    st.warning("⚠️ Anomaly detected: Significant drop in volume")
    st.write(anomalies)
else:
    st.success("No anomalies detected in transaction volume")
