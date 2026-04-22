import pandas as pd
import streamlit as st
from datetime import datetime, timezone

st.set_page_config(
    page_title="Payment Data Quality Dashboard",
    layout="wide",
    page_icon="📊"
)

st.title("📊 Payment Data Quality Dashboard")

# -------------------------
# Load data
# -------------------------
@st.cache_data
def load_data():
    df = pd.read_csv("data/output/payment_events.csv")
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce")
    df["date"] = df["event_timestamp"].dt.date
    return df

df = load_data()

# -------------------------
# Sidebar filters
# -------------------------
st.sidebar.header("Filters")

sources = st.sidebar.multiselect(
    "Source System",
    options=df["source_system"].dropna().unique(),
    default=df["source_system"].dropna().unique()
)

df = df[df["source_system"].isin(sources)]

# -------------------------
# KPI ROW (🔥 big upgrade)
# -------------------------
total_records = len(df)
duplicate_count = df.duplicated(subset=["event_id"]).sum()
null_rate = df.isnull().mean().mean() * 100

latest_ts = df["event_timestamp"].max()
now = datetime.now(timezone.utc)

freshness_mins = (
    (now - latest_ts).total_seconds() / 60
    if pd.notnull(latest_ts) else None
)

k1, k2, k3, k4 = st.columns(4)

k1.metric("Total Records", f"{total_records:,}")
k2.metric("Duplicate Events", duplicate_count)
k3.metric("Avg Null Rate (%)", f"{null_rate:.2f}")
k4.metric("Freshness (mins)", f"{freshness_mins:.1f}" if freshness_mins else "N/A")

st.divider()

# -------------------------
# Schema Compliance (Per Source)
# -------------------------
st.subheader("✅ Schema Compliance by Source")

def compute_compliance(group):
    total_cells = group.shape[0] * group.shape[1]
    null_cells = group.isnull().sum().sum()
    return 100 * (1 - null_cells / total_cells) if total_cells else 0

compliance = (
    df.groupby("source_system")
    .apply(compute_compliance)
    .reset_index(name="compliance_rate")
)

st.bar_chart(compliance.set_index("source_system"))

# -------------------------
# Data Freshness
# -------------------------
st.subheader("⏱️ Data Freshness by Source")

freshness = (
    df.groupby("source_system")["event_timestamp"]
    .max()
    .reset_index()
)

freshness["minutes_since_last_event"] = freshness["event_timestamp"].apply(
    lambda x: (now - x).total_seconds() / 60 if pd.notnull(x) else None
)

freshness["status"] = freshness["minutes_since_last_event"].apply(
    lambda x: "🔴 STALE" if x and x > 60 else "🟢 OK"
)

st.dataframe(freshness, use_container_width=True)

# -------------------------
# Volume Trend
# -------------------------
st.subheader("📉 Transaction Volume Trend")

volume = df.groupby(["date", "source_system"]).size().reset_index(name="count")

st.line_chart(volume, x="date", y="count", color="source_system")

# -------------------------
# Anomaly Detection
# -------------------------
st.subheader("🚨 Volume Anomalies")

alerts = []

for source, group in df.groupby("source_system"):
    daily = group.groupby("date").size()

    mean = daily.mean()
    std = daily.std()
    threshold = mean - 2 * std

    anomalies = daily[daily < threshold]

    if not anomalies.empty:
        alerts.append((source, anomalies))

if alerts:
    st.error("⚠️ Anomalies detected!")
    for source, anomaly in alerts:
        st.markdown(f"**{source}**")
        st.line_chart(anomaly)
else:
    st.success("No anomalies detected")

# -------------------------
# Null Analysis
# -------------------------
st.subheader("🧪 Null Rate by Column")

nulls = (
    df.isnull().mean()
    .sort_values(ascending=False)
    .reset_index()
)

nulls.columns = ["column", "null_rate"]

st.bar_chart(nulls.set_index("column"))

# -------------------------
# Status Distribution
# -------------------------
st.subheader("📊 Status Distribution")

status_dist = df["status"].value_counts()

st.bar_chart(status_dist)

# -------------------------
# Raw Data Explorer
# -------------------------
st.subheader("🔍 Data Explorer")

st.dataframe(df.head(200), use_container_width=True)
