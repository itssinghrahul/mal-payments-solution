"""
app.py — Mal Payments Unified Pipeline Demo
Run: streamlit run app.py
"""
import json
import pandas as pd
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Mal Payments — Unified Data Model",
    page_icon="💳",
    layout="wide",
)

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    df = pd.read_csv("data/output/payment_events.csv")
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
    df["date"] = df["event_timestamp"].dt.date
    return df

df = load_data()

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("## 💳 Mal Payments — Unified Payment Data Model")
st.markdown(
    "Three squads (Cards, Transfers, Bill Payments) had **inconsistent schemas**. "
    "This pipeline ingests all three, validates and normalises to a **canonical `PaymentEventV2` schema**."
)
st.divider()

# ── KPI row ───────────────────────────────────────────────────────────────────
total      = len(df)
success    = df[df["status"].isin(["APPROVED", "COMPLETED"])]["amount"].sum()
failed     = len(df[df["status"].isin(["FAILED", "DECLINED"])])
fee_total  = df["fee_amount"].sum()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Events",       total)
k2.metric("Successful Volume",  f"AED {success:,.2f}")
k3.metric("Failed / Declined",  failed)
k4.metric("Total Fees Collected", f"AED {fee_total:,.2f}")

st.divider()

# ── Squad breakdown ───────────────────────────────────────────────────────────
st.markdown("### Pipeline: 3 Squads → 1 Schema")

col1, col2 = st.columns([1, 2])

with col1:
    type_counts = df["payment_type"].value_counts().reset_index()
    type_counts.columns = ["Payment Type", "Count"]
    st.dataframe(type_counts, use_container_width=True, hide_index=True)

with col2:
    by_type = (
        df[df["status"].isin(["APPROVED", "COMPLETED"])]
        .groupby("payment_type")["amount"]
        .sum()
        .reset_index()
        .rename(columns={"payment_type": "Type", "amount": "Volume (AED)"})
    )
    st.bar_chart(by_type.set_index("Type"))

st.divider()

# ── Status breakdown ──────────────────────────────────────────────────────────
st.markdown("### Status Distribution")

col3, col4 = st.columns(2)

with col3:
    status_counts = df.groupby(["payment_type", "status"]).size().reset_index(name="count")
    st.dataframe(status_counts, use_container_width=True, hide_index=True)

with col4:
    daily = (
        df[df["status"].isin(["APPROVED", "COMPLETED"])]
        .groupby("date")["amount"]
        .sum()
        .reset_index()
        .rename(columns={"date": "Date", "amount": "Volume"})
    )
    st.line_chart(daily.set_index("Date"))

st.divider()

# ── Schema versioning ─────────────────────────────────────────────────────────
st.markdown("### Schema Versioning — v1 → v2")

col5, col6 = st.columns(2)

with col5:
    st.markdown("**v1.0 — Core fields**")
    st.code("""event_id, payment_type, customer_id
amount, currency, status
event_timestamp, payment_method
source_system, raw_reference
metadata, schema_version""", language="text")

with col6:
    st.markdown("**v2.0 — Additive fields (no breaking change)**")
    st.code("""# All v1 fields +
fee_amount       # default 0.0
fee_currency     # default = payment currency  
counterparty_name  # merchant / recipient / biller""", language="python")

ver_counts = df["schema_version"].value_counts().reset_index()
ver_counts.columns = ["Version", "Count"]
st.dataframe(ver_counts, use_container_width=True, hide_index=True)

st.divider()

# ── Live event explorer ───────────────────────────────────────────────────────
st.markdown("### Unified Event Explorer")

f1, f2, f3 = st.columns(3)
type_filter   = f1.multiselect("Payment Type", df["payment_type"].unique(),  default=list(df["payment_type"].unique()))
status_filter = f2.multiselect("Status",       df["status"].unique(),        default=list(df["status"].unique()))
source_filter = f3.multiselect("Source System",df["source_system"].unique(), default=list(df["source_system"].unique()))

filtered = df[
    df["payment_type"].isin(type_filter) &
    df["status"].isin(status_filter) &
    df["source_system"].isin(source_filter)
][["event_id","payment_type","customer_id","amount","currency",
   "status","event_timestamp","payment_method","source_system",
   "counterparty_name","fee_amount","schema_version"]]

st.dataframe(filtered, use_container_width=True, hide_index=True)
st.caption(f"{len(filtered)} of {total} events shown")

st.divider()

# ── Canonical schema reference ────────────────────────────────────────────────
with st.expander("📋 Canonical Schema Reference"):
    st.markdown("""
| Field | Type | Description |
|---|---|---|
| `event_id` | string | SHA-256 deterministic ID |
| `payment_type` | enum | CARD / TRANSFER / BILL |
| `customer_id` | string | Normalised customer ID |
| `amount` | float | Amount (2dp) |
| `currency` | string | ISO-4217 |
| `status` | enum | APPROVED / DECLINED / PENDING / FAILED / COMPLETED |
| `event_timestamp` | datetime | ISO-8601 UTC |
| `payment_method` | string | VISA / MASTERCARD / WITHIN_UAE / MOBILE_APP etc. |
| `source_system` | string | cards / transfers / bill_payments |
| `raw_reference` | string | Original source ID |
| `metadata` | dict | Source-specific fields |
| `fee_amount` | float | Fee (v2+) |
| `fee_currency` | string | Fee currency (v2+) |
| `counterparty_name` | string | Merchant / recipient / biller (v2+) |
""")

st.caption("Built with pandas · Python 3.9+ · Schema versioning via dataclasses · No proprietary dependencies")
