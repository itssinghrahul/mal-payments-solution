# Mal Payments — Unified Payment Data Model

A Python-based data pipeline that ingests inconsistent payment events from three product squads (Cards, Transfers, Bill Payments), normalises them into a canonical schema, and outputs a unified dataset for downstream analytics.

---

## Problem

Three squads built independent pipelines with incompatible schemas:

| Squad | Source Format | Key Inconsistencies |
|---|---|---|
| Cards | `txn_id`, `ccy`, `cust_id` | Split date/time fields, no fee concept |
| Transfers | `transfer_ref`, `initiated_at` (ISO8601) | Fee as separate field, IBAN accounts |
| Bill Payments | `bill_pay_id`, `payment_result` | Different status vocab (`SUCCESS` vs `APPROVED`) |

---

## Solution

A canonical `PaymentEventV2` schema that all three sources map to, with a versioned migration path for future schema evolution.

```
cards.csv  ──┐
             ├──▶  transform  ──▶  validate  ──▶  payment_events.json / .csv
transfers.csv─┤
             │
bill_payments─┘
```

---

## Canonical Schema (v2.0)

| Field | Type | Description |
|---|---|---|
| `event_id` | string | SHA-256 deterministic ID (source + ref + timestamp) |
| `schema_version` | string | `1.0` or `2.0` |
| `payment_type` | enum | `CARD` / `TRANSFER` / `BILL` |
| `customer_id` | string | Normalised customer identifier |
| `amount` | float | Payment amount (2 dp) |
| `currency` | string | ISO-4217 (e.g. `AED`, `USD`) |
| `status` | enum | `APPROVED` / `DECLINED` / `PENDING` / `FAILED` / `COMPLETED` |
| `event_timestamp` | datetime | ISO-8601 UTC |
| `payment_method` | string | `VISA`, `MASTERCARD`, `WITHIN_UAE`, `MOBILE_APP`, etc. |
| `source_system` | string | `cards` / `transfers` / `bill_payments` |
| `raw_reference` | string | Original ID from source system |
| `metadata` | dict | Source-specific fields (merchant, biller, account info) |
| `fee_amount` | float | Transaction fee (v2 only, defaults to `0.0`) |
| `fee_currency` | string | Fee currency (v2 only) |
| `counterparty_name` | string | Merchant / recipient / biller name (v2 only) |

---

## Schema Versioning (v1 → v2)

V1 defines the core 12 fields. V2 adds three fields without breaking V1 consumers:

```python
from schema import migrate_v1_to_v2

v2_event = migrate_v1_to_v2(
    v1_event,
    fee_amount=2.50,
    fee_currency="AED",
    counterparty_name="Ahmed Al Mansoori"
)
```

Every event carries a `schema_version` field so downstream systems can handle both versions without breaking.

---

## Project Structure

```
mal-payments-solution/
├── data/
│   ├── raw/                    # Input CSVs (mock squad data)
│   │   ├── cards.csv
│   │   ├── transfers.csv
│   │   └── bill_payments.csv
│   └── output/                 # Generated after running pipeline
│       ├── payment_events.json
│       ├── payment_events.csv
│       └── errors.json         # Only created if validation errors exist
├── schema.py                   # Canonical schema + v1/v2 dataclasses + migration
├── transformers.py             # Per-squad transformation logic
├── pipeline.py                 # Main entry point
├── queries.sql                 # Downstream analytics SQL
└── requirements.txt
```

---

## Setup & Run

**Requirements:** Python 3.9+

```bash
# 1. Clone the repo
git clone https://github.com/itssinghrahul/mal-payments-solution.git
cd mal-payments-solution

# 2. (Recommended) Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the pipeline
python3 pipeline.py
```

**Expected output:**
```
==================================================
  Pipeline complete — 2024-01-17 06:32 UTC
==================================================
  Total events : 22
  Total errors : 0
    CARD        : 8
    TRANSFER    : 6
    BILL        : 8
==================================================
```

Output files are written to `data/output/`.

---

## SQL Queries

`queries.sql` contains 5 ready-to-use queries for downstream teams:

1. **Daily volume & value by payment type**
2. **Failed / declined payments** — ops and risk alerting feed
3. **Customer 360** — spend summary across all payment types
4. **Fee revenue** — total fees collected by type
5. **Schema version distribution** — governance and migration tracking

Compatible with DuckDB, Spark SQL, Trino, or any ANSI SQL engine.

**Quick DuckDB example:**
```sql
-- install: pip install duckdb
-- run: python3 -c "import duckdb; duckdb.sql(\"SELECT * FROM 'data/output/payment_events.csv' LIMIT 5\").show()"
```

---

## Validation & Error Handling

Each event is validated before emission:
- `payment_type` must be `CARD`, `TRANSFER`, or `BILL`
- `status` must be one of the 5 canonical values
- `amount` must be non-negative
- `currency` must be 3-character ISO-4217
- `customer_id` cannot be empty

Invalid rows are written to `data/output/errors.json` with the source, original row, and error detail — they never reach the output dataset.

---

## Dependencies

| Package | Purpose |
|---|---|
| `pandas` | CSV ingestion and output |
| `pyarrow` | Parquet output (optional) |
| `duckdb` | Run SQL queries against output (optional) |

Core pipeline only requires `pandas`. All schema logic uses Python stdlib (`dataclasses`, `hashlib`, `datetime`).
