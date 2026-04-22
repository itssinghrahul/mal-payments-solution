## 1. Schema Compliance Rate (per source system)

Core metric: % of valid records per ingestion source

```
SELECT
    source_system,
    COUNT(*) AS total_records,

    -- assuming invalid rows are not ingested OR marked via null key fields
    SUM(CASE WHEN event_id IS NULL OR customer_id IS NULL OR amount IS NULL THEN 1 ELSE 0 END) AS invalid_records,

    ROUND(
        100.0 * SUM(CASE WHEN event_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS schema_compliance_rate
FROM payment_events
GROUP BY source_system;
```

## 2. Data Freshness (time since last ingestion)

Tracks pipeline health per system
```
SELECT
    source_system,
    MAX(event_timestamp) AS last_event_time,
    CURRENT_TIMESTAMP - MAX(event_timestamp) AS time_since_last_ingestion
FROM payment_events
GROUP BY source_system;
```
## 3. Anomaly Detection (transaction volume drop)

Detect sudden drops in activity per day

```
WITH daily_volume AS (
    SELECT
        source_system,
        DATE(event_timestamp) AS event_date,
        COUNT(*) AS txn_count
    FROM payment_events
    GROUP BY source_system, DATE(event_timestamp)
),

volume_with_lag AS (
    SELECT *,
           LAG(txn_count) OVER (
               PARTITION BY source_system
               ORDER BY event_date
           ) AS prev_day_count
    FROM daily_volume
)

SELECT
    source_system,
    event_date,
    txn_count,
    prev_day_count,
    ROUND(100.0 * (txn_count - prev_day_count) / NULLIF(prev_day_count, 0), 2) AS pct_change
FROM volume_with_lag
WHERE prev_day_count IS NOT NULL
  AND (txn_count < prev_day_count * 0.5 OR txn_count = 0)
ORDER BY event_date DESC;
```

## 4. Null Rate Analysis (data completeness)

Core data quality metric

```
SELECT
    source_system,

    ROUND(100.0 * SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS customer_id_null_rate,

    ROUND(100.0 * SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS amount_null_rate,

    ROUND(100.0 * SUM(CASE WHEN currency IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS currency_null_rate,

    ROUND(100.0 * SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS status_null_rate

FROM payment_events
GROUP BY source_system;
```
## 5. Transaction Volume & Value (baseline KPI)

Used in dashboard context + anomaly baseline

```
SELECT
    source_system,
    payment_type,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_volume,
    AVG(amount) AS avg_transaction_value
FROM payment_events
GROUP BY source_system, payment_type
ORDER BY total_volume DESC;
```

## 6. Failed / Declined Rate (data + business health)
```
SELECT
    source_system,
    COUNT(*) AS total_txns,
    SUM(CASE WHEN status IN ('FAILED', 'DECLINED') THEN 1 ELSE 0 END) AS failed_txns,
    ROUND(
        100.0 * SUM(CASE WHEN status IN ('FAILED', 'DECLINED') THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS failure_rate
FROM payment_events
GROUP BY source_system;
```

## 7. Fee Revenue Completeness (finance integrity check)
```
SELECT
    source_system,
    COUNT(*) AS total_txns,
    SUM(fee_amount) AS total_fees,
    SUM(CASE WHEN fee_amount IS NULL THEN 1 ELSE 0 END) AS missing_fee_count
FROM payment_events
GROUP BY source_system;
```
## 8. Customer Coverage (data distribution health)

Detect ingestion gaps or identity issues

```
SELECT
    source_system,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(*) AS total_events,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT customer_id), 2) AS avg_events_per_customer
FROM payment_events
GROUP BY source_system;
```

## 9. Schema Version Adoption (migration tracking)
```
SELECT
    schema_version,
    COUNT(*) AS event_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM payment_events
GROUP BY schema_version;
```
