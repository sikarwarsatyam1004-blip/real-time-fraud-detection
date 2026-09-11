# Real-Time Fraud Detection Pipeline

A production-style real-time Data Engineering project that processes simulated financial transactions using **Python, Apache Kafka, Apache Flink, ClickHouse, Prometheus, Grafana, Docker, and Slack**.

The pipeline generates financial transactions continuously, streams them through Kafka, processes them with Flink, performs fraud detection and window-based analysis, stores results in ClickHouse, visualizes the data in Grafana, monitors infrastructure using Prometheus, and sends operational alerts to Slack.

---

## Architecture

```text
                    +----------------------+
                    |   Python Producer    |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |    Apache Kafka      |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |    Apache Flink      |
                    |----------------------|
                    | Validation           |
                    | Transformation       |
                    | Event Time           |
                    | Watermarks           |
                    | Fraud Detection      |
                    | Window Aggregations  |
                    +----------+-----------+
                               |
                  +------------+------------+
                  |                         |
                  v                         v
       +---------------------+   +----------------------+
       | Processed           |   | Customer Window      |
       | Transactions        |   | Metrics              |
       +----------+----------+   +----------+-----------+
                  |                         |
                  +------------+------------+
                               |
                               v
                    +----------------------+
                    |     ClickHouse       |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |       Grafana        |
                    +----------------------+


Monitoring and Alerting:

     Flink JobManager / TaskManager
                 |
                 v
          +-------------+
          | Prometheus  |
          +------+------+
                 |
                 v
          +-------------+
          |   Grafana   |
          +------+------+
                 |
                 v
        +------------------+
        | Grafana Alerting |
        +--------+---------+
                 |
          +------+------+
          |             |
          v             v
   Local Webhook      Slack
```

---

## Technology Stack

| Layer | Technology |
|---|---|
| Data Generation | Python |
| Streaming Platform | Apache Kafka |
| Stream Processing | Apache Flink / PyFlink |
| Analytical Database | ClickHouse |
| Metrics Collection | Prometheus |
| Visualization | Grafana |
| Alerting | Grafana Alerting |
| Notifications | Slack |
| Containerization | Docker |
| Orchestration | Docker Compose |
| Version Control | Git / GitHub |

---

## Project Goals

The main goals of this project are to:

- Generate realistic financial transaction events
- Stream transactions through Apache Kafka
- Process events continuously using Apache Flink
- Validate and transform incoming transaction data
- Perform event-time based stream processing
- Handle late events using watermarks
- Calculate customer-level window aggregations
- Detect potentially fraudulent transactions
- Store processed results in ClickHouse
- Prevent duplicate analytical records
- Implement checkpoint-based fault tolerance
- Implement savepoint-based graceful recovery
- Monitor the streaming infrastructure with Prometheus
- Build production-style Grafana dashboards
- Configure operational alert rules
- Send real-time Grafana alerts to Slack

---

# Data Pipeline

## Python Transaction Producer

The Python producer generates simulated financial transactions continuously.

Each transaction contains fields such as:

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

The generated records are serialized and published to Apache Kafka.

The producer simulates realistic financial activity so that the streaming pipeline can be tested continuously.

---

## Apache Kafka

Apache Kafka acts as the messaging layer between the Python producer and Apache Flink.

Kafka is responsible for:

- Receiving transaction events
- Storing events inside Kafka topics
- Dividing data across partitions
- Maintaining consumer offsets
- Supporting replay of previously produced events
- Allowing Flink to consume events independently
- Supporting recovery after processing failures

Kafka runs inside Docker and communicates with the other services through the Docker Compose network.

---

# Apache Flink Processing

Apache Flink is the main real-time stream-processing engine in the project.

The running Flink application is:

```text
Fraud Detection ClickHouse Pipeline
```

The Flink pipeline performs:

- JSON parsing
- Data validation
- Data transformation
- Timestamp assignment
- Watermark generation
- Event-time processing
- Keyed stream processing
- Tumbling window aggregation
- Fraud scoring
- Fraud classification
- Checkpointing
- Recovery
- ClickHouse output processing

---

## Event-Time Processing

The pipeline uses **event time** instead of relying only on system processing time.

Event time represents when a transaction actually occurred.

This is important because real streaming systems may receive records:

- Late
- Out of order
- After temporary network delays
- After producer retries

Flink watermarks are used to track event-time progress and determine when event-time windows can safely be evaluated.

---

## Watermarks

Watermarks allow Flink to handle slightly delayed or out-of-order events.

They help the application determine when enough event-time progress has occurred to close a window.

This allows the pipeline to provide more realistic streaming behavior compared with simple processing-time logic.

---

## Window Processing

The project uses tumbling event-time windows to calculate customer-level activity.

Window metrics include:

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

These metrics help identify unusual transaction velocity and customer activity patterns.

---

# Fraud Detection

The pipeline applies rule-based fraud detection to processed transactions.

Potential fraud signals include:

- High transaction amounts
- Suspicious international transactions
- Rapid transaction activity
- Abnormal customer transaction velocity
- Suspicious merchant or category behavior

Processed transaction records contain fraud-related fields such as:

```text
is_fraud
fraud_score
fraud_reasons
```

This information is stored in ClickHouse and visualized through Grafana.

---

# ClickHouse Storage

ClickHouse is used as the analytical storage layer.

The main tables are:

```text
processed_transactions
customer_window_metrics
```

---

## Processed Transactions

The `processed_transactions` table stores enriched transaction data including:

- Event timestamp
- Transaction ID
- Customer ID
- Account ID
- Merchant
- Category
- Amount
- Currency
- Country
- City
- Device ID
- Payment method
- International transaction flag
- Validation status
- Fraud status
- Fraud score
- Fraud reasons
- Ingestion timestamp

---

## Customer Window Metrics

The `customer_window_metrics` table stores window-level customer aggregations.

It includes:

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

# ClickHouse Deduplication

The production tables use `ReplacingMergeTree` to support duplicate handling.

For transaction data:

```text
ReplacingMergeTree(ingested_at)
```

For window metrics:

```text
ReplacingMergeTree(processed_at)
```

The transaction table uses the logical transaction key for ordering.

Example validation query:

```sql
SELECT count()
FROM processed_transactions FINAL;
```

The number of unique transaction IDs can be checked using:

```sql
SELECT uniqExact(transaction_id)
FROM processed_transactions FINAL;
```

Duplicate IDs can be inspected with:

```sql
SELECT
    transaction_id,
    count() AS copies
FROM processed_transactions
GROUP BY transaction_id
HAVING copies > 1
ORDER BY copies DESC;
```

The final validation confirmed that deduplicated row counts matched the unique transaction count.

---

# Flink Checkpointing

Checkpointing is enabled to provide fault tolerance.

The configuration includes:

- Periodic checkpoints
- Exactly-once checkpoint mode
- Checkpoint timeout
- Minimum pause between checkpoints
- Maximum concurrent checkpoints
- Externalized checkpoint retention

Checkpoint storage location:

```text
file:///opt/flink/checkpoints
```

Checkpoints are persisted using Docker volumes so they survive container recreation.

---

## Checkpoint Validation

Checkpoint completion was validated through the Flink JobManager logs.

Example:

```text
Completed checkpoint 399
Completed checkpoint 400
Completed checkpoint 401
...
```

Hundreds of checkpoints completed successfully during validation.

Typical checkpoint durations remained low, with most checkpoints completing within milliseconds.

---

# Failure Recovery

The pipeline was tested by intentionally stopping the Flink TaskManager while the job was running.

Flink detected the failure and automatically restarted affected tasks.

The JobManager logs confirmed:

```text
RESTARTING
Restoring job from Checkpoint
Recovering subtask
RUNNING
Completed checkpoint
```

After recovery, the application returned to the `RUNNING` state and continued processing.

This validated Flink's checkpoint-based recovery behavior.

---

# Flink Savepoints

Savepoints are used for controlled shutdown and stateful restart.

A savepoint was created using Flink's stop-with-savepoint functionality.

Savepoint storage location:

```text
file:///opt/flink/savepoints
```

A saved Flink job can be restarted using:

```powershell
docker exec fraud-flink-jobmanager flink run -s file:/opt/flink/savepoints/<SAVEPOINT_NAME> -py /opt/flink/usrlib/fraud_job.py
```

Savepoint testing validated:

- Graceful shutdown
- State preservation
- Restart from saved state
- Controlled application recovery

---

# Docker Infrastructure

The entire project runs locally using Docker Compose.

Main containers include:

```text
fraud-kafka
fraud-flink-jobmanager
fraud-flink-taskmanager
fraud-clickhouse
fraud-grafana
fraud-prometheus
```

Docker Compose manages:

- Container lifecycle
- Docker networking
- Service dependencies
- Persistent volumes
- Health checks
- Restart policies
- Port mappings

---

## Restart Policies

Production-style restart policies are configured using:

```text
restart: unless-stopped
```

This allows services to restart automatically after unexpected termination or Docker restart.

---

## Health Checks

Health checks are configured for key services including:

- Kafka
- ClickHouse
- Flink JobManager
- Flink TaskManager
- Grafana
- Prometheus

Container health can be checked using:

```powershell
docker compose ps
```

---

# Prometheus Monitoring

Apache Flink exposes internal runtime metrics through the Prometheus metrics reporter.

The reporter uses:

```text
org.apache.flink.metrics.prometheus.PrometheusReporterFactory
```

Flink exposes metrics on port:

```text
9249
```

Prometheus scrapes:

```text
jobmanager:9249
taskmanager:9249
```

Prometheus can be opened at:

```text
http://localhost:9090
```

A simple health query is:

```promql
up
```

Expected Flink targets:

```text
flink-jobmanager = 1
flink-taskmanager = 1
```

---

# Grafana Dashboards

Grafana is available at:

```text
http://localhost:3000
```

The project contains two main dashboard areas:

1. Fraud analytics
2. Infrastructure and pipeline monitoring

---

## Real-Time Fraud Detection Dashboard

The fraud analytics dashboard reads data from ClickHouse.

It contains panels such as:

- Fraud percentage
- Fraud transaction amount
- Fraud transactions by country
- Fraud transactions by category
- Latest fraud alerts
- Velocity fraud alerts

This dashboard focuses on transaction and fraud analytics.

---

## Flink Production Monitoring Dashboard

The infrastructure dashboard reads Flink metrics through Prometheus.

Dashboard name:

```text
Flink Production Monitoring Dashboard
```

### Flink Health

- Flink Targets Up
- Healthy Flink Targets
- Available Task Slots
- Flink Job Restarts

### JVM Monitoring

- TaskManager JVM Heap Used
- TaskManager JVM Heap Max
- TaskManager JVM Heap Usage %
- TaskManager CPU Load

### Checkpoint Monitoring

- Checkpoint Duration
- Completed Checkpoints
- Failed Checkpoints
- Checkpoint Size

### Kafka Consumer Monitoring

- Kafka Consumer Poll Idle Ratio
- Kafka Commit Latency
- Kafka Records Consumed Rate
- Kafka Incoming Byte Rate
- Kafka Fetch Latency
- Kafka Fetch Rate

### Flink Processing Monitoring

- TaskManager Backpressure
- TaskManager Busy Time
- TaskManager Idle Time
- Flink Records In Rate
- Flink Records Out Rate
- Flink Records In Total
- Flink Records Out Total

### Network Monitoring

- TaskManager Network Input
- TaskManager Network Output

---

# Grafana Alerting

Grafana alert rules monitor important Flink production conditions.

The evaluation group is:

```text
Flink Production Alerts
```

The rules are evaluated every minute.

---

## Flink Target Down

The alert monitors Flink Prometheus targets.

Query:

```promql
up{job=~"flink-jobmanager|flink-taskmanager"}
```

Condition:

```text
Below 1
```

Severity:

```text
critical
```

The alert identifies the affected instance through Prometheus labels.

---

## Failed Checkpoints

Query:

```promql
flink_jobmanager_job_numberOfFailedCheckpoints
```

Condition:

```text
Above 0
```

Severity:

```text
warning
```

This alert identifies checkpoint failures that may affect application recovery.

---

## High JVM Heap Usage

Query:

```promql
100 * flink_taskmanager_Status_JVM_Memory_Heap_Used
/
flink_taskmanager_Status_JVM_Memory_Heap_Max
```

Condition:

```text
Above 80%
```

Severity:

```text
warning
```

This detects sustained TaskManager JVM memory pressure.

---

## High Checkpoint Duration

Query:

```promql
flink_jobmanager_job_lastCheckpointDuration
```

Condition:

```text
Above 5000 ms
```

Severity:

```text
warning
```

This identifies slow checkpoints that could increase recovery time.

---

## High Backpressure

Query:

```promql
max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)
```

Condition:

```text
Above 500 ms/s
```

Severity:

```text
warning
```

This detects sustained Flink processing backpressure.

---

# Alert Notification Messages

Grafana alert annotations contain real-time operational information.

Alert messages include:

- Alert name
- Current metric value
- Threshold
- Severity
- Service
- Flink instance
- Operational impact
- Recommended troubleshooting actions

Example:

```text
High Flink Backpressure Detected

Current value: 742 ms/s
Threshold: 500 ms/s
Severity: warning

Impact:
Processing may be slowing down and records may begin to accumulate.

Check:
- Downstream operators
- TaskManager CPU and memory
- Kafka throughput
- ClickHouse sink performance
- Processing bottlenecks
```

---

# Local Webhook Notification Testing

A lightweight Python HTTP server was used to validate Grafana webhook delivery.

The receiver listens on:

```text
localhost:5001
```

Because Grafana runs inside Docker, it reaches the Windows host through:

```text
http://host.docker.internal:5001
```

The receiver successfully accepted Grafana POST requests and returned:

```text
HTTP 200
```

This validated the Grafana webhook notification path.

---

# Slack Alert Integration

Grafana is integrated with Slack through a Slack contact point.

Alerts are delivered to the project's fraud monitoring Slack channel.

The alert flow is:

```text
Flink Metric
    |
    v
Prometheus
    |
    v
Grafana Alert Rule
    |
    v
Slack Contact Point
    |
    v
Slack Channel
```

A real High Backpressure alert was intentionally triggered during testing.

Slack successfully received:

```text
[FIRING:1] High Backpressure
```

This proved end-to-end alert delivery.

The production threshold was restored after testing.

---

# Production-Style Validation

The project went through several reliability and monitoring tests.

## Infrastructure Validation

Confirmed:

```text
Kafka                 Healthy
ClickHouse            Healthy
Flink JobManager      Healthy
Flink TaskManager     Healthy
Grafana               Healthy
Prometheus            Running
```

---

## Flink Job Validation

The Flink CLI confirmed:

```text
Fraud Detection ClickHouse Pipeline (RUNNING)
```

---

## Checkpoint Validation

Confirmed:

- Checkpoints triggered periodically
- Checkpoints completed successfully
- Checkpoint metadata persisted
- Checkpoints remained available after container recovery

---

## Failure Recovery Validation

Confirmed:

- TaskManager failure detection
- Automatic task restart
- Recovery from checkpoint
- Return to RUNNING state
- Continued checkpoint creation

---

## Savepoint Validation

Confirmed:

- Graceful stop
- Savepoint generation
- Savepoint persistence
- Job restart from savepoint

---

## ClickHouse Validation

Confirmed:

- Processed transaction storage
- Customer window metric storage
- Streaming inserts
- Duplicate inspection
- ReplacingMergeTree deduplication
- Correct FINAL query results

---

## Prometheus Validation

Confirmed:

- Flink Prometheus reporter loaded
- JobManager metrics exposed
- TaskManager metrics exposed
- Prometheus targets reachable
- Flink targets reported `up = 1`

---

## Grafana Validation

Confirmed:

- ClickHouse datasource
- Prometheus datasource
- Fraud analytics dashboard
- Flink infrastructure dashboard
- Real-time panel refresh
- JVM metrics
- Kafka consumer metrics
- Checkpoint metrics
- Backpressure metrics
- Processing throughput metrics

---

## Alerting Validation

Confirmed:

- Grafana alert rules evaluate successfully
- Normal and firing states work
- Local webhook contact point works
- Slack contact point works
- Real alert notification reaches Slack

---

# Useful URLs

| Service | URL |
|---|---|
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Flink Web UI | http://localhost:8081 |
| ClickHouse HTTP | http://localhost:8123 |
| Kafka | localhost:9092 |

---

# Starting the Project

Open PowerShell and move into the project:

```powershell
cd C:\Users\satyam.singh\real-time-fraud-detection
```

Start all Docker services:

```powershell
docker compose up -d
```

Verify them:

```powershell
docker compose ps
```

---

# Checking the Flink Job

Run:

```powershell
docker exec fraud-flink-jobmanager flink list
```

Expected output should include:

```text
Fraud Detection ClickHouse Pipeline (RUNNING)
```

---

# Starting the Flink Job

If no job is running, submit the PyFlink application:

```powershell
docker exec fraud-flink-jobmanager flink run -py /opt/flink/usrlib/fraud_job.py
```

If restoring from a savepoint:

```powershell
docker exec fraud-flink-jobmanager flink run -s file:/opt/flink/savepoints/<SAVEPOINT_NAME> -py /opt/flink/usrlib/fraud_job.py
```

---

# Checking Checkpoints

Run:

```powershell
docker compose logs --tail 100 jobmanager | Select-String "Completed checkpoint"
```

A healthy job should continue producing successful checkpoints.

---

# Checking Prometheus Targets

Open:

```text
http://localhost:9090
```

Run:

```promql
up
```

Both Flink targets should return:

```text
1
```

---

# Opening Grafana

Open:

```text
http://localhost:3000
```

Main dashboards:

```text
Real-Time Fraud Detection Dashboard
Flink Production Monitoring Dashboard
```

---

# Graceful Shutdown

For a normal development shutdown, stop the producer first.

For stateful Flink shutdown, create a savepoint before stopping the pipeline.

Then Docker services can be stopped with:

```powershell
docker compose stop
```

Persistent volumes should not be deleted unless a complete environment reset is intentionally required.

---

# Project Reliability Features

The final pipeline includes:

- Durable Kafka event streaming
- Kafka consumer offset management
- Flink event-time processing
- Watermarks
- Tumbling windows
- Fraud detection
- Customer-level aggregation
- Exactly-once checkpoint mode
- Externalized checkpoints
- Automatic restart strategy
- Checkpoint recovery
- Savepoint recovery
- ClickHouse ReplacingMergeTree deduplication
- Docker persistent volumes
- Docker health checks
- Docker restart policies
- Prometheus metrics collection
- Grafana infrastructure monitoring
- Grafana fraud analytics
- Production alert rules
- Local webhook notifications
- Slack alert notifications

---

# Project Status

| Component | Status |
|---|---|
| Python Transaction Producer | ✅ Complete |
| Kafka Streaming | ✅ Complete |
| PyFlink Processing | ✅ Complete |
| Validation and Transformation | ✅ Complete |
| Event-Time Processing | ✅ Complete |
| Watermarks | ✅ Complete |
| Window Aggregations | ✅ Complete |
| Fraud Detection | ✅ Complete |
| ClickHouse Storage | ✅ Complete |
| Grafana Fraud Dashboard | ✅ Complete |
| ClickHouse Deduplication | ✅ Complete |
| Flink Checkpointing | ✅ Complete |
| Failure Recovery | ✅ Complete |
| Savepoint Recovery | ✅ Complete |
| Docker Health Checks | ✅ Complete |
| Restart Policies | ✅ Complete |
| Prometheus Monitoring | ✅ Complete |
| Flink Production Dashboard | ✅ Complete |
| Grafana Alert Rules | ✅ Complete |
| Local Webhook Testing | ✅ Complete |
| Slack Alert Integration | ✅ Complete |
| Production Validation | ✅ Complete |

---

# Final Result

The project now provides an end-to-end local real-time streaming architecture:

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

with a separate production monitoring and alerting flow:

```text
Flink
   ↓
Prometheus
   ↓
Grafana
   ↓
Grafana Alerting
   ↓
Slack
```

The pipeline demonstrates real-time data generation, distributed event streaming, stateful stream processing, event-time semantics, fraud detection, analytical storage, fault tolerance, checkpoint recovery, savepoint recovery, deduplication, infrastructure monitoring, alerting, and real-time Slack notifications.

This project is designed as a practical end-to-end demonstration of modern **Data Engineering and real-time stream-processing concepts**.