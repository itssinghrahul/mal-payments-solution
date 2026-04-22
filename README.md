# Payments — Unified Payment Data Model

A Python-based data pipeline that ingests inconsistent payment events from three product squads (Cards, Transfers, Bill Payments), normalises them into a canonical schema, and produces a unified dataset for downstream analytics and reporting.

---

## Problem

Three independent squads built separate pipelines with incompatible schemas:

| Squad         | Source Format                            | Key Inconsistencies                            |
| ------------- | ---------------------------------------- | ---------------------------------------------- |
| Cards         | `txn_id`, `ccy`, `cust_id`               | Split date/time fields, no fee concept         |
| Transfers     | `transfer_ref`, `initiated_at` (ISO8601) | Fee as separate field, IBAN-based accounts     |
| Bill Payments | `bill_pay_id`, `payment_result`          | Different status vocab (`SUCCESS`, `APPROVED`) |

This fragmentation makes it difficult to:

* Run consistent analytics across payment types
* Maintain data quality
* Evolve schemas without breaking downstream consumers

---

## Architecture

```
cards.csv  ──┐
             ├──▶ transform ──▶ validate ──▶ unified output (JSON / CSV)
transfers.csv─┤
             │
bill_payments─┘
```

Each source has a dedicated transformer that maps raw fields into a shared canonical model.

---

## Canonical Schema (v2.0)

| Field               | Type     | Description                                                  |
| ------------------- | -------- | ------------------------------------------------------------ |
| `event_id`          | string   | Deterministic SHA-256 hash (source + reference + timestamp)  |
| `schema_version`    | string   | `1.0` or `2.0`                                               |
| `payment_type`      | enum     | `CARD` / `TRANSFER` / `BILL`                                 |
| `customer_id`       | string   | Normalised customer identifier                               |
| `amount`            | float    | Payment amount (2 decimal precision)                         |
| `currency`          | string   | ISO-4217 (e.g. `AED`, `USD`)                                 |
| `status`            | enum     | `APPROVED` / `DECLINED` / `PENDING` / `FAILED` / `COMPLETED` |
| `event_timestamp`   | datetime | ISO-8601 UTC                                                 |
| `payment_method`    | string   | `VISA`, `MASTERCARD`, `BANK_TRANSFER`, `MOBILE_APP`, etc.    |
| `source_system`     | string   | `cards` / `transfers` / `bill_payments`                      |
| `raw_reference`     | string   | Original ID from source system                               |
| `metadata`          | dict     | Source-specific attributes (merchant, biller, account info)  |
| `fee_amount`        | float    | Transaction fee (v2 only, default `0.0`)                     |
| `fee_currency`      | string   | Fee currency (v2 only)                                       |
| `counterparty_name` | string   | Merchant / recipient / biller name (v2 only)                 |

### Example Output

```json
{
  "event_id": "f3a1c9...",
  "payment_type": "CARD",
  "amount": 100.0,
  "currency": "AED",
  "status": "COMPLETED",
  "source_system": "cards"
}
```

---

## Design Decisions

### Canonical Schema

A unified schema standardizes events across heterogeneous systems, enabling consistent analytics and simplifying downstream consumption.

### Deterministic Event ID

`event_id` is generated using SHA-256 over key fields to:

* Ensure idempotency
* Prevent duplicate ingestion
* Support replayable pipelines

### Metadata for Flexibility

Source-specific attributes are stored in `metadata` to:

* Avoid frequent schema changes
* Keep the core model stable
* Allow flexible downstream use

### Validation at Ingestion

All records are validated before output:

* Prevents propagation of bad data
* Surfaces errors early
* Ensures reliability of downstream datasets

### Versioned Schema Evolution

Schema changes are handled via versioning (`v1 → v2`) to:

* Maintain backward compatibility
* Enable gradual migration
* Avoid breaking downstream systems

---

## Schema Versioning (v1 → v2)

V1 defines the core fields. V2 extends the schema with additional attributes without breaking compatibility.

```python
from schema import migrate_v1_to_v2

v2_event = migrate_v1_to_v2(
    v1_event,
    fee_amount=2.50,
    fee_currency="AED",
    counterparty_name="Ahmed Al Mansoori"
)
```

Each event includes a `schema_version` field, allowing consumers to handle multiple versions safely.

---

## Project Structure

```
mal-payments-solution/
├── data/
│   ├── raw/
│   │   ├── cards.csv
│   │   ├── transfers.csv
│   │   └── bill_payments.csv
│   └── output/
│       ├── payment_events.json
│       ├── payment_events.csv
│       └── errors.json
├── schema.py
├── transformers.py
├── pipeline.py
├── queries.sql
└── requirements.txt
```

---

## ⚙️ Setup & Execution

### 1. Prerequisites

- Python 3.9+
- pip installed
- git installed

Verify installation:

```bash
python3 --version
pip --version
git --version
```
### 2. Clone Repository

```bash
git clone https://github.com/itssinghrahul/mal-payments-solution.git
cd mal-payments-solution
```
### 3. Create Virtual Environment (Recommended)

```bash
python3 -m venv .venv
source .venv/bin/activate
```
### 4. Install Dependencies

```bash
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

## Run the ETL Pipeline

This runs ingestion, transformation, validation, and outputs unified dataset.

```bash
python3 pipeline.py
```

Output files:
```
data/output/payment_events.csv
data/output/payment_events.json
data/output/errors.json
```
---

## Output

```
==================================================
Pipeline complete — 2024-01-17 06:32 UTC
==================================================
Total events : 22
Total errors : 0
CARD         : 8
TRANSFER     : 6
BILL         : 8
==================================================
```

Outputs are written to `data/output/`.

---

## SQL Queries

`queries.sql` includes:

1. Daily volume & value by payment type
2. Failed / declined payments (ops monitoring)
3. Customer 360 (total spend)
4. Fee revenue by payment type
5. Schema version distribution

Compatible with DuckDB, Spark SQL, Trino, and other ANSI SQL engines.

---

## Validation & Error Handling

Each record is validated before emission:

* `payment_type` must be valid
* `status` must map to canonical values (e.g. `SUCCESS`, `APPROVED` → `COMPLETED`)
* `amount` must be non-negative
* `currency` must be ISO-4217 compliant
* `customer_id` cannot be empty

Invalid records are written to `data/output/errors.json` with detailed error context and excluded from final output.

---

## Trade-offs

* **Batch vs Streaming**
  Batch processing chosen for simplicity; streaming (Kafka + Flink/Spark) would be used in production.

* **JSON/CSV vs Parquet**
  Human-readable formats used here; columnar storage preferred for large-scale analytics.

* **Pandas vs Distributed Processing**
  Pandas is sufficient for small datasets; Spark/Flink would handle scale.

---

## Scaling Considerations

For production deployment:

* **Ingestion**: Kafka / Kinesis
* **Processing**: Spark Structured Streaming / Flink
* **Storage**: Partitioned Parquet in a data lake (S3/GCS)
* **Warehouse**: Snowflake / BigQuery
* **Orchestration**: Airflow / Dagster
* **Data Quality**: Great Expectations

---

## Dependencies

| Package | Purpose                                   |
| ------- | ----------------------------------------- |
| pandas  | CSV ingestion and transformation          |
| pyarrow | Optional Parquet output                   |
| duckdb  | Optional query engine for local analytics |

Core pipeline relies only on `pandas` and Python standard libraries.

