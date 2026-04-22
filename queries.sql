-- queries.sql
-- Downstream analytics queries against the unified payment_events table.
-- Compatible with DuckDB, Spark SQL, Trino, or any ANSI SQL engine.
-- Assumes: payment_events table loaded from payment_events.parquet

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Daily payment volume & value by type
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    DATE_TRUNC('day', CAST(event_timestamp AS TIMESTAMP)) AS event_date,
    payment_type,
    COUNT(*)                                               AS txn_count,
    SUM(amount)                                            AS total_amount,
    currency
FROM payment_events
WHERE status NOT IN ('FAILED', 'DECLINED')
GROUP BY 1, 2, 5
ORDER BY 1, 2;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Failed / declined payments (ops / risk alerting)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    event_id,
    payment_type,
    customer_id,
    amount,
    currency,
    status,
    event_timestamp,
    source_system,
    raw_reference
FROM payment_events
WHERE status IN ('FAILED', 'DECLINED')
ORDER BY event_timestamp DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Customer 360 — payment summary per customer
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    customer_id,
    COUNT(*)                                      AS total_payments,
    SUM(amount)                                   AS total_spend,
    COUNT(CASE WHEN payment_type = 'CARD'     THEN 1 END) AS card_txns,
    COUNT(CASE WHEN payment_type = 'TRANSFER' THEN 1 END) AS transfer_txns,
    COUNT(CASE WHEN payment_type = 'BILL'     THEN 1 END) AS bill_txns,
    COUNT(CASE WHEN status IN ('FAILED','DECLINED') THEN 1 END) AS failed_txns,
    MIN(event_timestamp)                          AS first_seen,
    MAX(event_timestamp)                          AS last_seen
FROM payment_events
GROUP BY customer_id
ORDER BY total_spend DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Fee revenue summary (transfers & bills)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    payment_type,
    source_system,
    COUNT(*)          AS txn_count,
    SUM(fee_amount)   AS total_fees,
    fee_currency
FROM payment_events
WHERE fee_amount > 0
GROUP BY 1, 2, 5
ORDER BY total_fees DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Schema version distribution (governance / migration tracking)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    schema_version,
    payment_type,
    COUNT(*) AS event_count,
    MIN(event_timestamp) AS earliest,
    MAX(event_timestamp) AS latest
FROM payment_events
GROUP BY 1, 2
ORDER BY 1, 2;
