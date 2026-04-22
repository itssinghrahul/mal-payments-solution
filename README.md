# 💳 Payments — Unified Payment Data Platform

A Python-based data platform that ingests inconsistent payment events from three product squads (Cards, Transfers, Bill Payments), normalises them into a canonical schema, and provides a unified dataset for analytics, data quality monitoring, and reporting.

---

# 🚨 Problem Statement

Three independent squads built separate payment pipelines with incompatible schemas:

| Squad         | Source Format                            | Key Issues |
|---------------|------------------------------------------|------------|
| Cards         | txn_id, ccy, cust_id                    | Split timestamps, no fee model |
| Transfers     | transfer_ref, initiated_at (ISO8601)    | Fee separated, IBAN-based accounts |
| Bill Payments | bill_pay_id, payment_result            | Inconsistent status vocabulary |

### Impact
- No unified analytics across payment types
- Broken cross-product reporting
- Hard to enforce data quality
- Difficult schema evolution

---

# 🧠 Solution Overview

We introduce a **canonical payment event model + ETL + data quality layer + dashboard**.


Raw Squad CSVs
│
▼
Python ETL Pipeline
(transform + validate)
│
▼
Canonical PaymentEventV2 Schema
│
├──────────────┐
▼ ▼
Analytics Layer Data Quality Dashboard (Streamlit)
(SQL-ready) (Observability)


---

# 📊 Canonical Schema (v2.0)

| Field | Type | Description |
|------|------|-------------|
| event_id | string | SHA-256 deterministic ID |
| schema_version | string | v1 / v2 |
| payment_type | enum | CARD / TRANSFER / BILL |
| customer_id | string | Unified customer identifier |
| amount | float | Transaction amount |
| currency | string | ISO-4217 |
| status | enum | APPROVED / DECLINED / PENDING / FAILED / COMPLETED |
| event_timestamp | datetime | UTC timestamp |
| payment_method | string | Channel or rail |
| source_system | string | cards / transfers / bill_payments |
| raw_reference | string | Original ID |
| metadata | dict | Source-specific fields |
| fee_amount | float | Transaction fee (v2+) |
| fee_currency | string | Fee currency |
| counterparty_name | string | Merchant / recipient / biller |

---

# 🔁 Schema Evolution (v1 → v2)

Backward-compatible migration approach:

```python
from schema import migrate_v1_to_v2

v2_event = migrate_v1_to_v2(
    v1_event,
    fee_amount=2.50,
    fee_currency="AED",
    counterparty_name="Merchant X"
)

Each record carries schema_version for safe downstream handling.

⚙️ Setup & Installation
1. Clone repo
git clone https://github.com/itssinghrahul/mal-payments-solution.git
cd mal-payments-solution
2. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate
3. Install dependencies
python3 -m pip install -r requirements.txt
🚀 Run the System
1. Run ETL Pipeline
python3 pipeline.py
Output:
data/output/payment_events.csv
data/output/payment_events.json
data/output/errors.json (if any)
2. Run Data Quality Dashboard (Streamlit)
python3 -m streamlit run app.py

Open:

http://localhost:8501
Dashboard includes:
Schema compliance rate
Data freshness per source
Transaction trends
Failure rates
Null rate monitoring
Volume anomaly detection
📊 SQL Analytics Layer

The unified dataset supports downstream SQL analytics:

Key Queries:
Daily volume & value by payment type
Failed / declined payments (risk monitoring)
Customer 360 analysis
Fee revenue tracking
Schema version adoption tracking
Data freshness monitoring
Null rate analysis

Compatible with DuckDB / Snowflake / BigQuery / Trino.

🧪 Validation & Data Quality

Each record is validated before ingestion:

Valid payment types enforced
Standardised status mapping
Non-negative amounts
ISO-4217 currency validation
Required customer_id enforcement

Invalid records are isolated in:

data/output/errors.json
📊 Data Quality Dimensions

This system tracks three core observability pillars:

1. Schema Compliance

Ensures ingestion correctness per source system

2. Data Freshness

Tracks pipeline latency per squad

3. Anomaly Detection

Detects:

sudden drops in transaction volume
spikes in null rates
abnormal distribution shifts
🧱 Design Decisions
Canonical Schema

Ensures consistency across heterogeneous systems

Metadata Field

Preserves flexibility without breaking core schema

Deterministic Event ID

Prevents duplication and ensures idempotency

Batch Processing

Simpler, reproducible ETL (streaming is future extension)

📈 Trade-offs
Area	Decision	Reason
Processing	Batch	Simplicity & reproducibility
Storage	CSV/JSON	Local demo clarity
Compute	Pandas	Lightweight vs Spark
Quality checks	Custom logic	Avoid external dependencies
🚀 Production Scaling Path

If scaled to production:

Ingestion → Kafka / Kinesis
Processing → Spark / Flink
Storage → S3 (Parquet)
Warehouse → Snowflake / BigQuery
Orchestration → Airflow / Dagster
Data Quality → Great Expectations / Soda
📁 Project Structure
mal-payments-solution/
├── data/
│   ├── raw/
│   └── output/
├── schema.py
├── transformers.py
├── pipeline.py
├── app.py
├── queries.sql
└── requirements.txt
🧾 Requirements

Core dependencies:

pandas
pyarrow
duckdb
pydantic
streamlit
🧠 Key Highlights
Multi-source ingestion (3 squads)
Canonical schema design
Schema versioning (v1 → v2)
Data validation layer
SQL analytics layer
Data quality dashboard (Streamlit)
Anomaly detection + freshness monitoring
🎯 Outcome

This project demonstrates:

End-to-end ETL pipeline design
Data modeling & normalization
Data quality & observability
Analytics enablement layer
Production thinking (scalability & trade-offs)
