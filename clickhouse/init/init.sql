CREATE DATABASE IF NOT EXISTS fraud_detection;

CREATE TABLE IF NOT EXISTS fraud_detection.processed_transactions
(
    event_time DateTime64(3, 'UTC'),

    transaction_id String,
    customer_id String,
    account_id String,

    merchant LowCardinality(String),
    category LowCardinality(String),

    amount Decimal(18, 2),
    currency LowCardinality(String),

    country LowCardinality(String),
    city LowCardinality(String),

    device_id String,
    payment_method LowCardinality(String),

    is_international UInt8,

    validation_status LowCardinality(String),

    is_fraud UInt8,
    fraud_score UInt8,
    fraud_reasons Array(String),

    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, customer_id, transaction_id);


CREATE TABLE IF NOT EXISTS fraud_detection.customer_window_metrics
(
    customer_id String,

    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),

    transaction_count UInt32,

    total_amount Decimal(18, 2),
    max_amount Decimal(18, 2),

    is_velocity_fraud UInt8,

    processed_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, customer_id);