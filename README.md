# Real-Time Fraud Detection Pipeline

A production-style **real-time Data Engineering project** that processes simulated financial transactions using **Python, Apache Kafka, Apache Flink, ClickHouse, Prometheus, Grafana, Docker, and Slack**.

The pipeline continuously generates financial transactions, streams them through Kafka, processes them using Flink, performs validation, event-time processing, watermark handling, fraud detection, and customer-level window aggregations, then stores the results in ClickHouse.

The project also includes production-style observability with Prometheus and Grafana, automated alerting, Slack notifications, checkpoint-based recovery, savepoints, ClickHouse deduplication, automatic Kafka topic initialization, and automatic Flink job submission.

The complete environment can be started with:

```powershell
docker compose up -d
```

---

# Table of Contents

- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Project Goals](#project-goals)
- [Quick Start](#quick-start)
- [Automatic Startup Flow](#automatic-startup-flow)
- [Project Structure](#project-structure)
- [Python Transaction Producer](#python-transaction-producer)
- [Apache Kafka](#apache-kafka)
- [Apache Flink](#apache-flink)
- [Event Time and Watermarks](#event-time-and-watermarks)
- [Window Processing](#window-processing)
- [Fraud Detection](#fraud-detection)
- [ClickHouse](#clickhouse)
- [ClickHouse Deduplication](#clickhouse-deduplication)
- [Checkpointing](#flink-checkpointing)
- [Failure Recovery](#failure-recovery)
- [Savepoints](#flink-savepoints)
- [Docker Infrastructure](#docker-infrastructure)
- [Prometheus Monitoring](#prometheus-monitoring)
- [Grafana Dashboards](#grafana-dashboards)
- [Grafana Alerting](#grafana-alerting)
- [Slack Integration](#slack-alert-integration)
- [End-to-End Smoke Test](#end-to-end-smoke-test)
- [Production Validation](#production-validation)
- [Useful URLs](#useful-urls)
- [Useful Commands](#useful-commands)
- [Troubleshooting](#troubleshooting)
- [Graceful Shutdown](#graceful-shutdown)
- [Interview Explanation](#interview-explanation)
- [Project Status](#project-status)

---

# Architecture

```text
                           +-----------------------+
                           |    Python Producer    |
                           |  Fake Transactions    |
                           +-----------+-----------+
                                       |
                                       | localhost:29092
                                       v
                           +-----------------------+
                           |    Apache Kafka       |
                           |  transactions topic   |
                           |     3 partitions      |
                           +-----------+-----------+
                                       |
                                       | kafka:9092
                                       v
                           +-----------------------+
                           |    Apache Flink       |
                           |-----------------------|
                           | JSON Validation       |
                           | Transformation        |
                           | Event Time            |
                           | Watermarks            |
                           | Fraud Detection       |
                           | Window Aggregations   |
                           | Checkpointing         |
                           +-----------+-----------+
                                       |
                         +-------------+-------------+
                         |                           |
                         v                           v
              +---------------------+     +----------------------+
              | Processed           |     | Customer Window      |
              | Transactions        |     | Metrics              |
              +----------+----------+     +----------+-----------+
                         |                           |
                         +-------------+-------------+
                                       |
                                       v
                           +-----------------------+
                           |      ClickHouse       |
                           | Analytical Storage    |
                           +-----------+-----------+
                                       |
                                       v
                           +-----------------------+
                           |       Grafana         |
                           | Fraud Analytics       |
                           +-----------------------+
```

## Monitoring and Alerting Architecture

```text
               Flink JobManager / TaskManager
                          |
                          | Prometheus metrics
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
                           v
                     +-----------+
                     |   Slack   |
                     +-----------+
```

---

# Technology Stack

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

# Project Goals

The project demonstrates how to build and operate a production-style streaming application locally.

Main goals:

- Generate realistic financial transaction events
- Stream transactions continuously through Kafka
- Process events with Apache Flink
- Validate incoming data
- Transform streaming records
- Apply event-time semantics
- Handle out-of-order events using watermarks
- Perform customer-level tumbling-window aggregations
- Detect potentially fraudulent transactions
- Store processed records in ClickHouse
- Handle duplicate records
- Implement checkpoint-based fault tolerance
- Support graceful savepoint shutdown and recovery
- Monitor Flink using Prometheus
- Build Grafana dashboards
- Configure production-style alerts
- Send real-time notifications to Slack
- Automatically create Kafka topics
- Automatically submit the Flink pipeline
- Support one-command startup using Docker Compose

---

# Quick Start

## Prerequisites

Install:

- Docker Desktop
- Git
- Python 3.10+
- PowerShell or another terminal
- VS Code recommended

---

## Clone the Repository

```powershell
git clone <repository-url>
cd real-time-fraud-detection
```

---

## Environment Variables

Create a local `.env` file if it does not already exist.

Example:

```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

The `.env` file must remain excluded from Git.

Do not commit:

- Slack webhook URLs
- passwords
- API tokens
- secrets

---

## Start the Complete Platform

```powershell
docker compose up -d
```

This automatically starts:

- Kafka
- Kafka topic initialization
- ClickHouse
- Flink JobManager
- Flink TaskManager
- Automatic Flink job submission
- Prometheus
- Grafana
- Grafana dashboard provisioning
- Grafana alert provisioning

---

## Verify Containers

```powershell
docker compose ps -a
```

Expected services include:

```text
fraud-kafka
fraud-kafka-init
fraud-clickhouse
fraud-flink-jobmanager
fraud-flink-taskmanager
fraud-flink-job-submitter
fraud-prometheus
fraud-grafana
```

The two initialization containers should normally finish with:

```text
fraud-kafka-init             Exited (0)
fraud-flink-job-submitter    Exited (0)
```

This is expected.

---

## Verify the Flink Pipeline

```powershell
docker exec fraud-flink-jobmanager flink list
```

Expected:

```text
Fraud Detection ClickHouse Pipeline (RUNNING)
```

---

# Automatic Startup Flow

The project is designed for one-command startup.

Running:

```powershell
docker compose up -d
```

automatically performs:

```text
Kafka starts
      |
      v
Kafka health check passes
      |
      v
kafka-init checks transactions topic
      |
      v
transactions topic created if missing
      |
      v
ClickHouse becomes healthy
      |
      v
Flink JobManager starts
      |
      v
Flink TaskManager registers
      |
      v
job-submitter waits for available slots
      |
      v
Checks whether fraud pipeline already exists
      |
      v
fraud_job.py submitted automatically
      |
      v
Fraud Detection ClickHouse Pipeline RUNNING
      |
      v
EXACTLY_ONCE checkpoints begin
      |
      v
Prometheus + Grafana monitoring available
```

No manual Kafka topic creation or manual `flink run` command is required during normal startup.

---

# Project Structure

```text
real-time-fraud-detection/
│
├── clickhouse/
│   └── init/
│
├── flink/
│   ├── Dockerfile
│   └── jobs/
│       └── fraud_job.py
│
├── grafana/
│   ├── dashboards/
│   │   ├── real-time-fraud-detection.json
│   │   └── flink-production-monitoring-dashboard.json
│   │
│   └── provisioning/
│       ├── alerting/
│       │   ├── alert-rules.yml
│       │   ├── contact-points.yml
│       │   └── notification-policies.yml
│       │
│       ├── dashboards/
│       │   └── dashboard.yml
│       │
│       └── datasources/
│
├── producer/
│   └── producer.py
│
├── prometheus/
│   └── prometheus.yml
│
├── docker-compose.yml
├── .env
├── .gitignore
└── README.md
```

---

# Python Transaction Producer

The Python producer generates simulated financial transactions continuously.

Example transaction fields:

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

The producer publishes records into Kafka.

---

## Kafka Connection from Windows

The Python producer runs on the Windows host and connects to Kafka using:

```text
localhost:29092
```

Example configuration:

```python
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092",
)
```

Start the producer:

```powershell
.\.venv\Scripts\python.exe producer\producer.py
```

Stop it with:

```text
Ctrl + C
```

---

# Apache Kafka

Kafka acts as the real-time messaging layer between the producer and Flink.

Kafka responsibilities include:

- Receiving transaction events
- Storing streaming events
- Partitioning data
- Maintaining consumer offsets
- Supporting replay
- Decoupling producers and consumers
- Supporting Flink recovery

---

## Kafka Topic

Main topic:

```text
transactions
```

Configuration:

```text
Partitions: 3
Replication Factor: 1
```

---

## Kafka Listener Configuration

Two Kafka listeners are used.

### Internal Docker Listener

Used by Flink and other Docker services:

```text
kafka:9092
```

### External Windows Listener

Used by the local Python producer:

```text
localhost:29092
```

This prevents Docker hostname resolution issues from Windows applications.

---

# Automatic Kafka Topic Initialization

The `kafka-init` container runs after Kafka becomes healthy.

It executes:

```text
Create transactions topic if it does not exist
Verify partitions
Verify replication factor
Exit successfully
```

The initialization service uses:

```text
--if-not-exists
```

so it is safe to run repeatedly.

Verify:

```powershell
docker compose logs kafka-init
```

Expected output includes:

```text
Checking Kafka topic: transactions
Kafka topic configuration:
PartitionCount: 3
ReplicationFactor: 1
Kafka initialization completed successfully.
```

---

# Apache Flink

Apache Flink is the real-time processing engine.

The running application is:

```text
Fraud Detection ClickHouse Pipeline
```

The pipeline performs:

- Kafka consumption
- JSON parsing
- Data validation
- Data transformation
- Timestamp assignment
- Event-time processing
- Watermark generation
- Fraud scoring
- Fraud classification
- Keyed processing
- Tumbling-window aggregation
- ClickHouse writes
- Exactly-once checkpointing

---

# Automatic Flink Job Submission

The project includes a one-time Docker service:

```text
fraud-flink-job-submitter
```

It:

1. Waits for the JobManager
2. Checks whether the fraud pipeline is already running
3. Waits for available TaskManager slots
4. Avoids duplicate job submission
5. Runs `fraud_job.py`
6. Verifies the job reaches `RUNNING`
7. Exits with code `0`

Verify:

```powershell
docker compose logs job-submitter
```

Typical successful output:

```text
Flink JobManager is reachable.
No running Fraud Detection ClickHouse Pipeline found.
Available slots: 2
TaskManager has enough available slots.
Submitting Fraud Detection ClickHouse Pipeline...
Flink submission command succeeded.
Fraud Detection ClickHouse Pipeline is RUNNING.
```

---

# Event Time and Watermarks

The pipeline processes transactions using **event time**.

Event time represents when an event actually happened rather than when Flink happened to process it.

This matters because events can arrive:

- late
- out of order
- after network delays
- after retries

Flink assigns timestamps from:

```text
event_time
```

and uses watermarks to determine stream progress.

The configured bounded out-of-orderness allowance is:

```text
5 seconds
```

---

# Window Processing

The pipeline performs customer-level tumbling event-time aggregation.

Current window size:

```text
1 minute
```

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

Window processing helps detect unusual transaction velocity.

---

# Fraud Detection

The pipeline applies rule-based fraud detection.

Current example fraud signals include:

- High transaction amount
- Elevated transaction amount
- High-value international transaction
- High-risk transaction category
- Suspicious payment method
- High transaction velocity

Processed transactions contain:

```text
is_fraud
fraud_score
fraud_reasons
```

---

## Example Controlled Fraud Event

During the final smoke test, a controlled transaction was injected:

```text
Amount:             7500 USD
International:      true
Merchant:           Luxury Electronics Store
```

Flink produced:

```text
validation_status:  VALID
is_fraud:           1
fraud_score:        80
```

Fraud reasons:

```text
high_transaction_amount
high_value_international_transaction
```

This validated the full fraud-detection path.

---

# ClickHouse

ClickHouse provides analytical storage for the streaming pipeline.

Main tables:

```text
processed_transactions
customer_window_metrics
```

---

## processed_transactions

Stores enriched transaction-level data including:

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
- International flag
- Validation status
- Fraud status
- Fraud score
- Fraud reasons
- Ingestion timestamp

---

## customer_window_metrics

Stores customer-level window calculations:

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

The project uses `ReplacingMergeTree` tables for duplicate handling.

Processed transactions use:

```text
ReplacingMergeTree(ingested_at)
```

Window metrics use:

```text
ReplacingMergeTree(processed_at)
```

---

## Validate Deduplication

Compare physical and logical counts:

```sql
SELECT count()
FROM processed_transactions FINAL;
```

```sql
SELECT uniqExact(transaction_id)
FROM processed_transactions FINAL;
```

Inspect physical duplicate IDs:

```sql
SELECT
    transaction_id,
    count() AS copies
FROM processed_transactions
GROUP BY transaction_id
HAVING copies > 1
ORDER BY copies DESC;
```

The final validation confirmed logical deduplication worked as expected.

---

# Flink Checkpointing

Checkpointing provides stateful fault tolerance.

The job explicitly enables:

```text
Mode: EXACTLY_ONCE
Interval: 30000 ms
Timeout: 60000 ms
Minimum pause: 10000 ms
Maximum concurrent checkpoints: 1
Externalized checkpoints: enabled
Retention: RETAIN_ON_CANCELLATION
```

Checkpoint storage:

```text
file:///opt/flink/checkpoints
```

Persistent Docker volume:

```text
flink-checkpoints
```

---

## Checkpoint Configuration

The PyFlink application uses:

```python
env.enable_checkpointing(
    30000,
    CheckpointingMode.EXACTLY_ONCE,
)
```

and retains checkpoints externally when the job is cancelled.

---

## Verify Checkpoint Configuration

Get the running job:

```powershell
$overview = docker exec fraud-flink-jobmanager curl -s http://localhost:8081/jobs/overview | ConvertFrom-Json

$job = $overview.jobs |
    Where-Object { $_.state -eq "RUNNING" } |
    Select-Object -First 1
```

Then:

```powershell
docker exec fraud-flink-jobmanager curl -s "http://localhost:8081/jobs/$($job.jid)/checkpoints/config"
```

Expected values:

```text
mode           : exactly_once
interval       : 30000
timeout        : 60000
min_pause      : 10000
max_concurrent : 1
```

---

## Check Checkpoint Counts

```powershell
$checkpoints = docker exec fraud-flink-jobmanager curl -s "http://localhost:8081/jobs/$($job.jid)/checkpoints" | ConvertFrom-Json

$checkpoints.counts
```

A healthy pipeline should show:

```text
completed   > 0
failed      = 0
```

During validation, hundreds of checkpoints completed successfully with zero failures.

---

# Failure Recovery

Fault tolerance was tested by intentionally disrupting the Flink TaskManager.

The recovery flow was:

```text
TaskManager failure
      |
      v
JobManager detects failure
      |
      v
Job enters RESTARTING
      |
      v
State restored from checkpoint
      |
      v
Tasks rescheduled
      |
      v
Pipeline returns to RUNNING
```

This verified Flink checkpoint-based recovery.

---

# Flink Savepoints

Savepoints support controlled application shutdown and stateful restart.

Savepoint storage:

```text
file:///opt/flink/savepoints
```

Persistent Docker volume:

```text
flink-savepoints
```

---

## Graceful Savepoint Shutdown

Example:

```powershell
docker exec fraud-flink-jobmanager flink stop -p file:/opt/flink/savepoints <JOB_ID>
```

Restart from savepoint:

```powershell
docker exec fraud-flink-jobmanager flink run `
  -s file:/opt/flink/savepoints/<SAVEPOINT_NAME> `
  -py /opt/flink/usrlib/fraud_job.py
```

Savepoint testing validated:

- Graceful shutdown
- State preservation
- Restart from saved state
- Stateful upgrade workflow

---

# Docker Infrastructure

The entire environment runs using Docker Compose.

Main services:

```text
kafka
kafka-init
clickhouse
jobmanager
taskmanager
job-submitter
prometheus
grafana
```

Docker Compose manages:

- Container startup
- Networking
- Service dependencies
- Health checks
- Persistent volumes
- Port exposure
- Restart policies
- Initialization containers

---

# Docker Health Checks

Health checks are configured for:

- Kafka
- ClickHouse
- Flink JobManager
- Flink TaskManager
- Prometheus
- Grafana

Check:

```powershell
docker compose ps -a
```

---

# Restart Policies

Long-running services use:

```text
restart: unless-stopped
```

Initialization services use:

```text
restart: "no"
```

because they are designed to complete once and exit successfully.

---

# Persistent Volumes

Persistent data includes:

```text
real-time-fraud-detection_kafka-data
real-time-fraud-detection_clickhouse-data
real-time-fraud-detection_grafana-data
real-time-fraud-detection_flink-checkpoints
real-time-fraud-detection_flink-savepoints
real-time-fraud-detection_prometheus-data
```

Persistent volumes allow the environment to survive container recreation.

---

# Prometheus Monitoring

Flink exposes runtime metrics using the Prometheus reporter.

Reporter:

```text
org.apache.flink.metrics.prometheus.PrometheusReporterFactory
```

Metrics port:

```text
9249
```

Prometheus scrapes:

```text
jobmanager:9249
taskmanager:9249
```

Prometheus UI:

```text
http://localhost:9090
```

---

## Verify Targets

Run:

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

Grafana:

```text
http://localhost:3000
```

Two main dashboards are included:

```text
Real-Time Fraud Detection Dashboard
Flink Production Monitoring Dashboard
```

Both dashboards are stored as JSON and provisioned automatically when Grafana starts.

---

# Real-Time Fraud Detection Dashboard

This dashboard reads fraud analytics from ClickHouse.

Panels include:

- Total Transactions
- Fraud Transactions
- Fraud Rate
- Total Transaction Value
- Fraud by Country
- Fraud by Category
- Latest Fraud Alerts
- Velocity Fraud Alerts

During the final validation, the deliberately injected fraud transaction appeared in the dashboard.

This proved:

```text
Kafka
  ↓
Flink
  ↓
ClickHouse
  ↓
Grafana
```

end to end.

---

# Flink Production Monitoring Dashboard

This dashboard uses Prometheus.

## Flink Health

- Flink Targets Up
- Healthy Flink Targets
- Available Task Slots
- Flink Job Restarts

## JVM Monitoring

- TaskManager JVM Heap Used
- TaskManager JVM Heap Max
- TaskManager JVM Heap Usage %
- TaskManager CPU Load

## Checkpoint Monitoring

- Checkpoint Duration
- Completed Checkpoints
- Failed Checkpoints
- Checkpoint Size

## Kafka Consumer Monitoring

- Kafka Consumer Poll Idle Ratio
- Kafka Commit Latency
- Kafka Records Consumed Rate
- Kafka Incoming Byte Rate
- Kafka Fetch Latency
- Kafka Fetch Rate

## Flink Processing Monitoring

- TaskManager Backpressure
- TaskManager Busy Time
- TaskManager Idle Time
- Flink Records In Rate
- Flink Records Out Rate
- Flink Records In Total
- Flink Records Out Total

## Network Monitoring

- TaskManager Network Input
- TaskManager Network Output

---

# Grafana Dashboard Provisioning

Dashboard provisioning configuration is stored under:

```text
grafana/provisioning/dashboards
```

Dashboard JSON files are stored in:

```text
grafana/dashboards
```

This allows dashboards to automatically return after Grafana container recreation.

---

# Grafana Alerting

The project contains five core Flink production alerts.

Alert evaluation group:

```text
Flink Production Alerts
```

Evaluation interval:

```text
1 minute
```

---

## 1. Flink Target Down

PromQL:

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

---

## 2. Failed Checkpoints

PromQL:

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

---

## 3. High JVM Heap Usage

PromQL:

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

---

## 4. High Checkpoint Duration

PromQL:

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

---

## 5. High Backpressure

PromQL:

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

---

# Alert Notification Messages

Alert messages include dynamic operational details such as:

- Alert name
- Current metric value
- Threshold
- Severity
- Service
- Instance
- Impact
- Troubleshooting recommendations

Example:

```text
High Flink Backpressure Detected

Current value: 742 ms/s
Threshold: 500 ms/s
Severity: warning

Impact:
Processing may be slowing down and records may accumulate.

Check:
- Downstream operators
- TaskManager CPU and memory
- Kafka throughput
- ClickHouse sink performance
- Processing bottlenecks
```

---

# Grafana Alert Provisioning

Alert configuration is stored under:

```text
grafana/provisioning/alerting
```

Files include:

```text
alert-rules.yml
contact-points.yml
notification-policies.yml
```

This makes alert configuration reproducible through Git.

---

# Slack Alert Integration

Grafana sends production alerts to Slack.

The notification path is:

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

Slack successfully received the firing notification.

The alert threshold was restored after validation.

---

# Slack Secret Management

The Slack webhook is stored only in:

```text
.env
```

Example:

```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

Grafana provisioning references:

```text
${SLACK_WEBHOOK_URL}
```

The real webhook should never be committed.

Verify:

```powershell
git grep "hooks.slack.com"
```

Expected result:

```text
No output
```

---

# Local Webhook Testing

Before Slack integration, Grafana notifications were validated using a local Python HTTP receiver.

Local receiver:

```text
localhost:5001
```

Grafana reached the host from Docker using:

```text
http://host.docker.internal:5001
```

Grafana successfully delivered HTTP POST requests and received:

```text
HTTP 200
```

This validated Grafana contact-point functionality.

---

# End-to-End Smoke Test

The final system was tested end to end.

Validated flow:

```text
Python Producer
      |
      v
Kafka
      |
      v
Flink
      |
      v
ClickHouse
      |
      v
Grafana
```

---

## Producer to Kafka Validation

The Python producer generated live transaction JSON.

A Kafka console consumer successfully read live messages from:

```text
transactions
```

Example result:

```text
Processed a total of 5 messages
```

This proved:

```text
Python Producer → Kafka
```

---

## Kafka to Flink Validation

The Flink CLI confirmed:

```text
Fraud Detection ClickHouse Pipeline (RUNNING)
```

Kafka records were consumed continuously by Flink.

---

## Flink to ClickHouse Validation

Before the smoke test:

```text
processed_transactions = 271
customer_window_metrics = 186
```

After producing new transactions:

```text
processed_transactions > 271
customer_window_metrics > 186
```

This proved both transaction-level processing and window aggregation.

---

## Controlled Fraud Validation

A guaranteed fraud transaction was injected directly into Kafka.

Example:

```text
transaction_id:
smoke_fraud_...

customer:
cust_smoke_test

amount:
7500 USD

international:
true
```

Flink produced:

```text
validation_status = VALID
is_fraud = 1
fraud_score = 80
```

Reasons:

```text
high_transaction_amount
high_value_international_transaction
```

The transaction appeared in ClickHouse and the Grafana Latest Fraud Alerts panel.

This validated:

```text
Controlled Event
     ↓
Kafka
     ↓
Flink Validation
     ↓
Fraud Rules
     ↓
ClickHouse
     ↓
Grafana
```

---

# Production Validation

The system underwent multiple reliability tests.

---

## Infrastructure Validation

Confirmed healthy:

```text
Kafka
ClickHouse
Flink JobManager
Flink TaskManager
Prometheus
Grafana
```

Initialization containers completed successfully:

```text
kafka-init             Exited (0)
job-submitter          Exited (0)
```

---

## Kafka Fresh-Volume Recovery Test

The Kafka data volume was deliberately removed while keeping other persistent data intact.

After:

```powershell
docker compose up -d
```

the system automatically:

1. Created a fresh Kafka broker
2. Created `transactions`
3. Configured 3 partitions
4. Started Flink
5. Submitted the fraud pipeline
6. Started checkpointing

This proved Kafka initialization is reproducible from an empty state.

---

## Flink Automatic Submission Validation

The submitter successfully detected:

```text
No running Fraud Detection ClickHouse Pipeline found.
```

waited for TaskManager slots, and submitted the job.

Final state:

```text
Fraud Detection ClickHouse Pipeline (RUNNING)
```

---

## Checkpoint Validation

Verified configuration:

```text
mode           : exactly_once
interval       : 30000
timeout        : 60000
min_pause      : 10000
max_concurrent : 1
```

Hundreds of checkpoints completed successfully with:

```text
failed = 0
```

---

## Failure Recovery Validation

Confirmed:

- TaskManager failure detection
- Job restart
- Checkpoint restoration
- State recovery
- Return to RUNNING
- Continued checkpointing

---

## Savepoint Validation

Confirmed:

- Graceful stop
- Savepoint generation
- Savepoint persistence
- Restart from savepoint
- Restored application state

---

## ClickHouse Validation

Confirmed:

- Transaction inserts
- Window metric inserts
- Analytical queries
- Fraud queries
- Duplicate inspection
- ReplacingMergeTree deduplication
- `FINAL` logical results

---

## Prometheus Validation

Confirmed:

- Flink reporter enabled
- JobManager metrics available
- TaskManager metrics available
- Prometheus targets healthy
- `up = 1`

---

## Grafana Validation

Confirmed:

- ClickHouse datasource
- Prometheus datasource
- Fraud dashboard
- Flink monitoring dashboard
- Real-time refresh
- JVM metrics
- Kafka metrics
- Checkpoint metrics
- Backpressure metrics
- Processing metrics

---

## Alerting Validation

Confirmed:

- Alert evaluation
- Normal state
- Pending state
- Firing state
- Local webhook delivery
- Slack delivery
- Dynamic alert messages

---

# Useful URLs

| Service | URL |
|---|---|
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Flink Web UI | http://localhost:8081 |
| ClickHouse HTTP | http://localhost:8123 |
| Kafka Windows Listener | localhost:29092 |
| Kafka Docker Listener | kafka:9092 |

---

# Useful Commands

## Start Everything

```powershell
docker compose up -d
```

---

## Check All Containers

```powershell
docker compose ps -a
```

---

## Stop Everything

```powershell
docker compose stop
```

---

## Remove Containers Without Volumes

```powershell
docker compose down
```

---

## Check Flink Job

```powershell
docker exec fraud-flink-jobmanager flink list
```

---

## Check Kafka Topics

```powershell
docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --list
```

---

## Describe Kafka Topic

```powershell
docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --describe `
  --topic transactions
```

---

## Consume Five Kafka Records

```powershell
docker exec fraud-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic transactions `
  --from-beginning `
  --max-messages 5 `
  --timeout-ms 30000
```

---

## Start Python Producer

```powershell
.\.venv\Scripts\python.exe producer\producer.py
```

---

## Check Transaction Count

```powershell
docker exec fraud-clickhouse clickhouse-client `
  --user fraud_user `
  --password change_me `
  --query "SELECT count() FROM fraud_detection.processed_transactions"
```

---

## Check Fraud Count

```powershell
docker exec fraud-clickhouse clickhouse-client `
  --user fraud_user `
  --password change_me `
  --query "SELECT count() FROM fraud_detection.processed_transactions WHERE is_fraud = 1"
```

---

## Check Window Metrics Count

```powershell
docker exec fraud-clickhouse clickhouse-client `
  --user fraud_user `
  --password change_me `
  --query "SELECT count() FROM fraud_detection.customer_window_metrics"
```

---

## Check Submitter Logs

```powershell
docker compose logs job-submitter
```

---

## Check Kafka Initialization Logs

```powershell
docker compose logs kafka-init
```

---

## Check Grafana Logs

```powershell
docker compose logs --tail 200 grafana
```

---

## Check JobManager Logs

```powershell
docker compose logs --tail 200 jobmanager
```

---

## Check TaskManager Logs

```powershell
docker compose logs --tail 200 taskmanager
```

---

# Troubleshooting

## Flink Shows No Running Jobs

Check:

```powershell
docker compose logs job-submitter
```

Then:

```powershell
docker exec fraud-flink-jobmanager flink list
```

If the submitter already completed, recreate it:

```powershell
docker compose rm -f job-submitter

docker compose up job-submitter
```

---

## Kafka Topic Missing

Check:

```powershell
docker compose logs kafka-init
```

Verify:

```powershell
docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --list
```

The `kafka-init` service should automatically create `transactions`.

---

## Windows Producer Cannot Reach Kafka

Verify:

```powershell
Test-NetConnection localhost -Port 29092
```

Expected:

```text
TcpTestSucceeded : True
```

The Windows producer must use:

```text
localhost:29092
```

not:

```text
kafka:9092
```

---

## Docker Services Must Use Internal Kafka Address

Docker services should use:

```text
kafka:9092
```

This includes Flink.

---

## Grafana Shows No Data

Check the dashboard time range.

Recommended:

```text
Last 5 minutes
```

Then verify Prometheus:

```promql
up
```

---

## Checkpoints Show Zero

Check effective configuration:

```powershell
docker exec fraud-flink-jobmanager curl -s `
  "http://localhost:8081/jobs/$($job.jid)/checkpoints/config"
```

Expected interval:

```text
30000
```

Expected mode:

```text
exactly_once
```

---

## Prometheus Checkpoint Metric Is Empty

Verify the Flink job is running:

```powershell
docker exec fraud-flink-jobmanager flink list
```

Job-specific metrics may disappear when no job is active.

---

## Task Slots Show Zero Available

If:

```text
slots-total     = 2
slots-available = 0
jobs-running    = 1
```

this can be normal.

The running Flink application uses both configured slots.

---

# Graceful Shutdown

For normal development:

1. Stop the Python producer with:

```text
Ctrl + C
```

2. Optionally create a Flink savepoint for stateful shutdown.

3. Stop services:

```powershell
docker compose stop
```

Do not use:

```powershell
docker compose down -v
```

unless you intentionally want to delete persistent state.

---

# Project Reliability Features

The project includes:

- Kafka durable event streaming
- Kafka consumer offset tracking
- Three Kafka partitions
- Separate internal/external Kafka listeners
- Automatic Kafka topic initialization
- Automatic Flink job submission
- Duplicate-submission prevention
- Flink event-time processing
- Watermarks
- Tumbling windows
- Rule-based fraud detection
- Customer-level aggregations
- EXACTLY_ONCE checkpoint mode
- Externalized checkpoints
- Fixed-delay restart strategy
- Checkpoint recovery
- Savepoint recovery
- ClickHouse ReplacingMergeTree deduplication
- Docker persistent volumes
- Docker health checks
- Docker restart policies
- Prometheus monitoring
- Grafana dashboards
- File-based Grafana provisioning
- Production alert rules
- Slack notifications
- Dynamic alert messages
- One-command startup

---

# Interview Explanation

## 30-Second Version

This project is an end-to-end real-time fraud detection pipeline. A Python producer generates financial transactions and publishes them to Kafka. Apache Flink consumes the events, validates them, applies event-time processing, watermarks, fraud scoring, and one-minute customer-level window aggregations. Results are stored in ClickHouse and visualized in Grafana. Flink is configured with EXACTLY_ONCE checkpoints, checkpoint recovery, and savepoints. Prometheus monitors the Flink cluster, Grafana evaluates production alert rules, and alerts are delivered to Slack. The whole environment runs through Docker Compose and supports automatic Kafka topic creation and Flink job submission.

---

## 2-Minute Version

The project simulates a production-style financial transaction streaming platform.

A Python producer continuously generates transaction events containing customer, merchant, amount, location, payment method, international flags, and event timestamps.

The producer writes these events to a three-partition Kafka topic. Kafka provides durable event storage, partitioning, consumer offsets, and replay capabilities.

Apache Flink consumes Kafka events using PyFlink. The pipeline validates each event, assigns event-time timestamps, generates watermarks for out-of-order handling, applies rule-based fraud scoring, and performs one-minute tumbling-window aggregations by customer.

Processed transactions and window-level metrics are written to ClickHouse. The tables use ReplacingMergeTree engines to support logical deduplication.

For reliability, Flink runs with EXACTLY_ONCE checkpointing every 30 seconds, externalized checkpoint retention, restart strategies, and savepoint support.

Prometheus scrapes JobManager and TaskManager metrics. Grafana provides both business-level fraud analytics and infrastructure-level monitoring. Five production alert rules monitor target availability, failed checkpoints, memory usage, checkpoint duration, and backpressure. Alerts are delivered to Slack.

The platform is fully containerized and supports one-command startup with automatic Kafka topic initialization and automatic Flink job submission.

---

# Key Data Engineering Concepts Demonstrated

## Kafka

- Topics
- Partitions
- Brokers
- Consumer groups
- Offsets
- Event replay
- Internal vs external listeners
- Producer/consumer decoupling

## Flink

- DataStream API
- Stateful processing
- Event time
- Watermarks
- Keyed streams
- Tumbling windows
- Parallelism
- Task slots
- Checkpointing
- Recovery
- Savepoints
- Backpressure
- Prometheus metrics

## ClickHouse

- Columnar analytics
- MergeTree family
- ReplacingMergeTree
- Real-time analytical inserts
- Aggregation queries
- Logical deduplication

## Observability

- Prometheus scraping
- Grafana dashboards
- JVM metrics
- Kafka consumer metrics
- Checkpoint metrics
- Backpressure monitoring
- Alert evaluation
- Slack notifications

## Docker

- Multi-container applications
- Service dependencies
- Health checks
- Persistent volumes
- Internal DNS
- Initialization services
- Restart policies

---

# Resume-Ready Project Summary

**Real-Time Fraud Detection / Transaction Monitoring Platform**

- Built an end-to-end real-time streaming pipeline using **Python, Kafka, PyFlink, ClickHouse, Prometheus, Grafana, Docker, and Slack**.
- Implemented **event-time processing, watermarks, one-minute tumbling windows, fraud scoring, and customer-level stream aggregations**.
- Configured Flink **EXACTLY_ONCE checkpoints every 30 seconds**, externalized checkpoint retention, failure recovery, and savepoint-based restart.
- Designed ClickHouse analytical tables using **ReplacingMergeTree** for duplicate handling and low-latency fraud analytics.
- Built Grafana dashboards for **fraud metrics, JVM health, Kafka consumer performance, checkpoints, throughput, and backpressure**.
- Implemented production alerts for **target availability, checkpoint failures, JVM memory, checkpoint duration, and backpressure**, with Slack delivery.
- Automated Kafka topic initialization and Flink job submission, enabling the platform to start using a single `docker compose up -d` command.

---

# Project Status

| Component | Status |
|---|---|
| Python Transaction Producer | ✅ Complete |
| Kafka Streaming | ✅ Complete |
| Kafka 3-Partition Topic | ✅ Complete |
| Kafka Automatic Topic Creation | ✅ Complete |
| Internal/External Kafka Listeners | ✅ Complete |
| PyFlink Processing | ✅ Complete |
| Automatic Flink Job Submission | ✅ Complete |
| Validation and Transformation | ✅ Complete |
| Event-Time Processing | ✅ Complete |
| Watermarks | ✅ Complete |
| Window Aggregations | ✅ Complete |
| Fraud Detection | ✅ Complete |
| Controlled Fraud Smoke Test | ✅ Complete |
| ClickHouse Storage | ✅ Complete |
| ClickHouse Deduplication | ✅ Complete |
| Flink EXACTLY_ONCE Checkpointing | ✅ Complete |
| Externalized Checkpoints | ✅ Complete |
| Failure Recovery | ✅ Complete |
| Savepoint Recovery | ✅ Complete |
| Docker Health Checks | ✅ Complete |
| Docker Persistent Volumes | ✅ Complete |
| Restart Policies | ✅ Complete |
| Prometheus Monitoring | ✅ Complete |
| Real-Time Fraud Dashboard | ✅ Complete |
| Flink Production Dashboard | ✅ Complete |
| Grafana Dashboard Provisioning | ✅ Complete |
| Grafana Alert Provisioning | ✅ Complete |
| Five Production Alert Rules | ✅ Complete |
| Local Webhook Testing | ✅ Complete |
| Slack Alert Integration | ✅ Complete |
| Fresh Kafka Recovery Test | ✅ Complete |
| One-Command Startup | ✅ Complete |
| End-to-End Smoke Test | ✅ Complete |
| Production Validation | ✅ Complete |

---

# Final Result

The final real-time processing path is:

```text
Python Producer
      ↓
Apache Kafka
      ↓
Apache Flink
      ↓
ClickHouse
      ↓
Grafana
```

The monitoring and alerting path is:

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

The reliability path is:

```text
Flink State
    ↓
EXACTLY_ONCE Checkpoints
    ↓
Persistent Docker Volume
    ↓
Failure Recovery / Savepoints
```

The automated startup path is:

```text
docker compose up -d
        ↓
Kafka
        ↓
Automatic Topic Creation
        ↓
Flink Cluster
        ↓
Automatic Job Submission
        ↓
Fraud Pipeline RUNNING
        ↓
Prometheus + Grafana
        ↓
Slack Alerts
```

This project demonstrates a complete production-style real-time Data Engineering workflow including **event streaming, stateful stream processing, event-time semantics, fraud detection, analytical storage, fault tolerance, deduplication, observability, alerting, infrastructure automation, and reproducible local deployment**.