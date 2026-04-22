# 💳 Payments --- Unified Payment Data Platform

A Python-based data platform that ingests inconsistent payment events
from three product squads (Cards, Transfers, Bill Payments), normalises
them into a canonical schema, and provides a unified dataset for
analytics, data quality monitoring, and reporting.

------------------------------------------------------------------------

# 🚨 Problem Statement

Three independent squads built separate payment pipelines with
incompatible schemas:

  --------------------------------------------------------------------------
  Squad           Source Format                               Key Issues
  --------------- ------------------------------------------- --------------
  Cards           txn_id, ccy, cust_id                        Split
                                                              timestamps, no
                                                              fee model

  Transfers       transfer_ref, initiated_at (ISO8601)        Fee separated,
                                                              IBAN-based
                                                              accounts

  Bill Payments   bill_pay_id, payment_result                 Inconsistent
                                                              status
                                                              vocabulary
  --------------------------------------------------------------------------

### Impact

-   No unified analytics across payment types
-   Broken cross-product reporting
-   Hard to enforce data quality
-   Difficult schema evolution

------------------------------------------------------------------------

# 🧠 Solution Overview

Raw Squad CSVs → Python ETL Pipeline (transform + validate) → Canonical
PaymentEventV2 Schema → Unified Dataset (CSV / JSON) → SQL Analytics +
Streamlit Dashboard (Observability)

------------------------------------------------------------------------

# 📊 Canonical Schema (v2.0)

-   event_id: SHA-256 deterministic ID
-   schema_version: v1 / v2
-   payment_type: CARD / TRANSFER / BILL
-   customer_id: unified customer identifier
-   amount: transaction amount
-   currency: ISO-4217
-   status: APPROVED / DECLINED / FAILED / COMPLETED / PENDING
-   event_timestamp: UTC timestamp
-   payment_method: payment rail
-   source_system: cards / transfers / bill_payments
-   raw_reference: original system ID
-   metadata: source-specific fields
-   fee_amount: v2+
-   fee_currency: v2+
-   counterparty_name: v2+

------------------------------------------------------------------------

# 🔁 Schema Evolution (v1 → v2)

from schema import migrate_v1_to_v2

v2_event = migrate_v1_to_v2( v1_event, fee_amount=2.50,
fee_currency="AED", counterparty_name="Merchant X" )

------------------------------------------------------------------------

# ⚙️ Setup & Run

git clone https://github.com/itssinghrahul/mal-payments-solution.git cd
mal-payments-solution

python3 -m venv .venv source .venv/bin/activate

python3 -m pip install -r requirements.txt

python3 pipeline.py

------------------------------------------------------------------------

# 🚀 Run Dashboard

python3 -m streamlit run app.py

Open: http://localhost:8501

------------------------------------------------------------------------

# 📊 Dashboard Metrics

-   Schema compliance rate
-   Data freshness
-   Transaction volume trends
-   Failure rate
-   Null rate analysis
-   Anomaly detection

------------------------------------------------------------------------

# 🧪 Data Validation

-   payment_type validation
-   status normalization
-   non-negative amounts
-   ISO currency validation
-   required customer_id

Invalid records → data/output/errors.json

------------------------------------------------------------------------

# 📊 Data Quality Metrics

1.  Schema Compliance Rate
2.  Data Freshness
3.  Anomaly Detection

------------------------------------------------------------------------

# 📈 Trade-offs

-   Batch processing over streaming
-   Pandas over distributed compute
-   JSON/CSV over Parquet (demo simplicity)

------------------------------------------------------------------------

# 🚀 Production Scaling

Kafka → Spark/Flink → S3 → Snowflake → Airflow → Great Expectations
