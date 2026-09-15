# Real-Time Fraud Detection Pipeline Architecture

## Overview

This document describes the final architecture of the Real-Time Fraud Detection Pipeline.

The platform implements a production-style streaming architecture using:

- Python
- Apache Kafka
- Apache Flink / PyFlink
- ClickHouse
- Prometheus
- Grafana
- Slack
- Docker Compose

The system supports:

- Real-time transaction streaming
- Stateful stream processing
- Event-time processing
- Fraud detection
- Window aggregation
- Analytical storage
- Monitoring
- Alerting
- Fault recovery


---

# High-Level Architecture


```text

                         DATA GENERATION LAYER

                    +----------------------+
                    |  Python Producer     |
                    |----------------------|
                    | Synthetic Banking    |
                    | Transactions        |
                    +----------+-----------+

                               |
                               |
                               v


                         STREAMING LAYER

                    +----------------------+
                    |     Apache Kafka     |
                    |----------------------|
                    | Topic: transactions |
                    | Partitions: 3       |
                    | Consumer Offsets   |
                    | Event Replay        |
                    +----------+-----------+

                               |
                               |
                               v


                    STREAM PROCESSING LAYER

                    +----------------------+
                    |    Apache Flink      |
                    |----------------------|
                    | PyFlink DataStream   |
                    | JSON Validation      |
                    | Transformation       |
                    | Event Time           |
                    | Watermarks           |
                    | Fraud Detection      |
                    | Window Aggregation   |
                    | Checkpointing        |
                    +----------+-----------+

                               |
                 +-------------+-------------+
                 |                           |
                 v                           v


        +-------------------+       +----------------------+
        | Processed         |       | Customer Window      |
        | Transactions      |       | Metrics              |
        |                   |       | Analytics            |
        +---------+---------+       +----------+-----------+

                  \                       /
                   \                     /
                    \                   /
                     v                 v


                     ANALYTICAL STORAGE

                    +----------------------+
                    |     ClickHouse       |
                    |----------------------|
                    | Real-Time Analytics  |
                    | ReplacingMergeTree   |
                    | Fraud Investigation  |
                    +----------+-----------+

                               |
                               |
                               v


                     VISUALIZATION LAYER

                    +----------------------+
                    |       Grafana        |
                    |----------------------|
                    | Fraud Dashboard      |
                    | Transaction Metrics  |
                    | Flink Monitoring     |
                    +----------------------+



                     OBSERVABILITY PIPELINE


                    +----------------------+
                    | Flink Metrics        |
                    |----------------------|
                    | JobManager           |
                    | TaskManager          |
                    +----------+-----------+

                               |
                               v


                    +----------------------+
                    |     Prometheus       |
                    |----------------------|
                    | Metrics Collection   |
                    | Time-Series Storage  |
                    +----------+-----------+

                               |
                               v


                    +----------------------+
                    | Grafana Dashboards   |
                    | Grafana Alerting     |
                    +----------+-----------+

                               |
                               v


                    +----------------------+
                    |       Slack          |
                    | Operational Alerts   |
                    +----------------------+



                     RELIABILITY LAYER


                    +----------------------+
                    | Flink Checkpoints    |
                    |----------------------|
                    | EXACTLY_ONCE         |
                    | State Recovery       |
                    | Fault Tolerance      |
                    +----------------------+


                    +----------------------+
                    | Flink Savepoints     |
                    |----------------------|
                    | Controlled Restart   |
                    | Stateful Upgrade     |
                    +----------------------+

```

---

# Data Flow

The complete transaction processing flow:

```text
Python Producer

        |
        v

Kafka transactions topic

        |
        v

Apache Flink

        |
        +-----------------------------+
        |                             |
        v                             v

Processed Transactions        Customer Window Metrics

        |
        v

ClickHouse

        |
        v

Grafana Fraud Dashboard
```

---

# Streaming Layer

## Python Producer

The producer generates realistic financial transaction events.

Each event contains:

```text
transaction_id
customer_id
account_id
merchant
category
amount
currency
country
city
device_id
payment_method
is_international
ip_address
event_time
```

The producer publishes transactions continuously into Kafka.


---

# Apache Kafka

Kafka acts as the durable streaming layer.

Responsibilities:

- Receive transaction events
- Store events in topics
- Maintain partitions
- Track consumer offsets
- Support replay
- Decouple producers and consumers


Kafka configuration:

```text
Topic:

transactions


Partitions:

3


Replication:

1
```

Kafka listeners:

```text
Docker Internal:

kafka:9092


Windows Producer:

localhost:29092
```

---

# Apache Flink Processing Layer

Apache Flink performs real-time stream processing.

The running application:

```text
Fraud Detection ClickHouse Pipeline
```

Processing steps:

```text
Kafka Source

      |
      v

JSON Parsing

      |
      v

Validation

      |
      v

Transformation

      |
      v

Event Time Assignment

      |
      v

Watermark Generation

      |
      v

Fraud Detection

      |
      v

Window Aggregation

      |
      v

ClickHouse Sink
```

---

# Event-Time Processing

The pipeline uses event time instead of processing time.

Event time represents when the transaction actually occurred.

This allows handling:

- Late events
- Out-of-order events
- Network delays
- Producer retries


Flink uses:

```text
event_time
```

for timestamp assignment.


---

# Watermark Processing

Watermarks allow Flink to understand stream progress.

They help determine when windows can safely close.

Benefits:

- Handles delayed events
- Supports realistic streaming behavior
- Improves accuracy of time-based calculations


---

# Fraud Detection Layer

The pipeline applies rule-based fraud detection.

Fraud signals include:

```text
High transaction amount

High-value international transaction

Suspicious category

Suspicious payment method

Transaction velocity
```

Fraud output fields:

```text
is_fraud

fraud_score

fraud_reasons
```

Example:

```text
Amount:
7500 USD


International:
true


Fraud Score:
80


Fraud Status:
true
```

---

# Window Aggregation

The pipeline performs customer-level tumbling event-time windows.

Window size:

```text
1 minute
```

Metrics:

```text
customer_id

window_start

window_end

transaction_count

total_amount

max_amount

is_velocity_fraud

processed_at
```

These metrics help detect unusual customer activity patterns.

---

# Analytical Storage

## ClickHouse

ClickHouse stores processed streaming results.

Main tables:

```text
processed_transactions

customer_window_metrics
```

---

## processed_transactions

Stores:

```text
event_time

transaction_id

customer_id

account_id

merchant

category

amount

currency

country

city

device_id

payment_method

is_international

validation_status

is_fraud

fraud_score

fraud_reasons

ingested_at
```

---

## customer_window_metrics

Stores:

```text
customer_id

window_start

window_end

transaction_count

total_amount

max_amount

is_velocity_fraud

processed_at
```

---

# Deduplication Strategy

ClickHouse uses:

```text
ReplacingMergeTree
```

for duplicate handling.

Transaction table:

```text
ReplacingMergeTree(ingested_at)
```

Window metrics:

```text
ReplacingMergeTree(processed_at)
```

This supports:

- Duplicate detection
- Logical deduplication
- Reliable analytical queries


---

# Reliability Architecture


## Flink Checkpointing

The pipeline uses:

```text
EXACTLY_ONCE
```

checkpoint mode.

Configuration:

```text
Checkpoint Interval:

30 seconds


Timeout:

60 seconds


Maximum Concurrent Checkpoints:

1
```

Checkpoint storage:

```text
file:///opt/flink/checkpoints
```

Purpose:

- Preserve state
- Recover after failures
- Prevent data loss


---

## Flink Savepoints

Savepoints support controlled restart.

Used for:

- Graceful shutdown
- Stateful upgrade
- Application migration


Storage:

```text
file:///opt/flink/savepoints
```

---

# Monitoring Architecture


```text
Flink JobManager
        |
        |
        v

Flink TaskManager
        |
        |
        v

Prometheus

        |
        |
        v

Grafana

        |
        |
        v

Slack Alerts
```

---

# Prometheus Monitoring

Prometheus collects:

- Flink runtime metrics
- JVM metrics
- Kafka metrics
- Checkpoint metrics
- Processing metrics


Tracked components:

```text
JobManager

TaskManager

Kafka Consumer

Checkpoint System
```

---

# Grafana Dashboards


## Fraud Detection Dashboard

Provides:

- Total transactions
- Fraud count
- Fraud percentage
- Transaction value
- Fraud by country
- Fraud by category
- Latest fraud alerts
- Velocity alerts


---

## Flink Production Monitoring Dashboard

Provides:

### Health

- Flink targets
- Healthy targets
- Task slots
- Job restarts


### JVM

- Heap usage
- Heap maximum
- CPU usage


### Kafka

- Consumer rate
- Fetch latency
- Commit latency


### Checkpoints

- Duration
- Size
- Completed checkpoints
- Failed checkpoints


### Processing

- Records in
- Records out
- Backpressure
- Busy time


---

# Alerting Architecture


```text
Flink Metrics

      |
      v

Prometheus

      |
      v

Grafana Alert Rules

      |
      v

Slack Notification
```

Alerts include:

- Flink target down
- Failed checkpoints
- High JVM memory
- High checkpoint duration
- High backpressure


---

# Deployment Architecture


The complete platform runs using Docker Compose.

Services:

```text
Kafka

Kafka Init

Flink JobManager

Flink TaskManager

Flink Job Submitter

ClickHouse

Prometheus

Grafana
```

Startup:

```text
docker compose up -d
```

Automatic flow:

```text
Kafka starts

      |
      v

Topic created automatically

      |
      v

Flink cluster starts

      |
      v

Job submitted automatically

      |
      v

Monitoring enabled

      |
      v

Alerts active
```

---

# Final Architecture Summary


## Data Pipeline

```text
Python

  ↓

Kafka

  ↓

Flink

  ↓

ClickHouse

  ↓

Grafana
```


## Monitoring Pipeline

```text
Flink

  ↓

Prometheus

  ↓

Grafana

  ↓

Slack
```


## Reliability Pipeline

```text
Flink State

  ↓

EXACTLY_ONCE Checkpoints

  ↓

Recovery


Flink Savepoints

  ↓

Controlled Restart
```

---

# Key Capabilities

- Real-time transaction streaming
- Kafka-based event ingestion
- Stateful stream processing
- Event-time processing
- Watermarks
- Fraud detection
- Window aggregation
- Analytical storage
- Deduplication
- Exactly-once processing
- Fault recovery
- Savepoint recovery
- Infrastructure monitoring
- Grafana dashboards
- Slack alerting
- Automated deployment