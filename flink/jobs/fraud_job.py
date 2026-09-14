import base64
import json
import os
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pyflink.common import Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
    ExternalizedCheckpointRetention,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    MapFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import TumblingEventTimeWindows


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "transactions",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "fraud-detection-flink",
)

CLICKHOUSE_HOST = os.getenv(
    "CLICKHOUSE_HOST",
    "clickhouse",
)

CLICKHOUSE_PORT = int(
    os.getenv(
        "CLICKHOUSE_PORT",
        "8123",
    )
)

CLICKHOUSE_DATABASE = os.getenv(
    "CLICKHOUSE_DATABASE",
    "fraud_detection",
)

CLICKHOUSE_USER = os.getenv(
    "CLICKHOUSE_USER",
    "fraud_user",
)

CLICKHOUSE_PASSWORD = os.getenv(
    "CLICKHOUSE_PASSWORD",
    "change_me",
)


REQUIRED_FIELDS = [
    "transaction_id",
    "customer_id",
    "account_id",
    "merchant",
    "category",
    "amount",
    "currency",
    "country",
    "city",
    "device_id",
    "payment_method",
    "event_time",
]


def utc_now_datetime():
    return datetime.now(timezone.utc)


def clickhouse_datetime(value=None):
    if value is None:
        value = utc_now_datetime()

    if isinstance(value, datetime):
        dt = value
    else:
        timestamp = parse_event_time(value)

        if timestamp is None:
            dt = utc_now_datetime()
        else:
            dt = datetime.fromtimestamp(
                timestamp / 1000,
                tz=timezone.utc,
            )

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    dt = dt.astimezone(timezone.utc)

    return dt.strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )[:-3]


def parse_event_time(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        number = float(value)

        if number > 100000000000:
            return int(number)

        return int(number * 1000)

    text = str(value).strip()

    if not text:
        return None

    try:
        number = float(text)

        if number > 100000000000:
            return int(number)

        return int(number * 1000)

    except ValueError:
        pass

    try:
        normalized = text.replace(
            "Z",
            "+00:00",
        )

        dt = datetime.fromisoformat(
            normalized
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return int(
            dt.timestamp() * 1000
        )

    except Exception:
        return None


def validate_transaction(raw_message):
    try:
        transaction = json.loads(
            raw_message
        )

        if not isinstance(
            transaction,
            dict,
        ):
            return None

        missing_fields = [
            field
            for field in REQUIRED_FIELDS
            if field not in transaction
            or transaction[field] is None
            or transaction[field] == ""
        ]

        validation_errors = []

        if missing_fields:
            validation_errors.append(
                "missing_fields:"
                + ",".join(
                    missing_fields
                )
            )

        try:
            transaction["amount"] = float(
                transaction.get(
                    "amount",
                    0,
                )
            )

            if transaction["amount"] < 0:
                validation_errors.append(
                    "negative_amount"
                )

        except (
            TypeError,
            ValueError,
        ):
            transaction["amount"] = 0.0

            validation_errors.append(
                "invalid_amount"
            )

        event_timestamp = parse_event_time(
            transaction.get(
                "event_time"
            )
        )

        if event_timestamp is None:
            validation_errors.append(
                "invalid_event_time"
            )

            event_timestamp = int(
                utc_now_datetime()
                .timestamp()
                * 1000
            )

        transaction[
            "event_timestamp"
        ] = event_timestamp

        transaction[
            "event_time"
        ] = clickhouse_datetime(
            event_timestamp
        )

        transaction[
            "is_international"
        ] = bool(
            transaction.get(
                "is_international",
                False,
            )
        )

        transaction[
            "validation_errors"
        ] = validation_errors

        transaction[
            "validation_status"
        ] = (
            "VALID"
            if len(validation_errors) == 0
            else "INVALID"
        )

        transaction[
            "is_fraud"
        ] = False

        transaction[
            "fraud_score"
        ] = 0

        transaction[
            "fraud_reasons"
        ] = []

        return transaction

    except Exception as exc:
        print(
            "Transaction validation error:",
            str(exc),
        )

        return None


def calculate_fraud(transaction):
    fraud_score = 0
    fraud_reasons = []

    amount = float(
        transaction.get(
            "amount",
            0,
        )
    )

    is_international = bool(
        transaction.get(
            "is_international",
            False,
        )
    )

    category = str(
        transaction.get(
            "category",
            "",
        )
    ).lower()

    payment_method = str(
        transaction.get(
            "payment_method",
            "",
        )
    ).lower()

    if amount >= 5000:
        fraud_score += 50

        fraud_reasons.append(
            "high_transaction_amount"
        )

    elif amount >= 2500:
        fraud_score += 25

        fraud_reasons.append(
            "elevated_transaction_amount"
        )

    if (
        is_international
        and amount >= 2000
    ):
        fraud_score += 30

        fraud_reasons.append(
            "high_value_international_transaction"
        )

    high_risk_categories = {
        "gambling",
        "crypto",
        "cryptocurrency",
        "cash_advance",
    }

    if category in high_risk_categories:
        fraud_score += 20

        fraud_reasons.append(
            "high_risk_category"
        )

    suspicious_payment_methods = {
        "unknown",
        "anonymous",
    }

    if (
        payment_method
        in suspicious_payment_methods
    ):
        fraud_score += 20

        fraud_reasons.append(
            "suspicious_payment_method"
        )

    fraud_score = min(
        fraud_score,
        100,
    )

    transaction[
        "fraud_score"
    ] = fraud_score

    transaction[
        "fraud_reasons"
    ] = fraud_reasons

    transaction[
        "is_fraud"
    ] = (
        fraud_score >= 50
    )

    return transaction


class ValidateTransactionMap(
    MapFunction
):

    def map(
        self,
        value,
    ):
        return validate_transaction(
            value
        )


class FraudDetectionMap(
    MapFunction
):

    def map(
        self,
        value,
    ):
        return calculate_fraud(
            value
        )


class EventTimestampAssigner(
    TimestampAssigner
):

    def extract_timestamp(
        self,
        value,
        record_timestamp,
    ):
        timestamp = value.get(
            "event_timestamp"
        )

        if timestamp is None:
            return int(
                utc_now_datetime()
                .timestamp()
                * 1000
            )

        return int(timestamp)


class CustomerWindowAggregate(
    AggregateFunction
):

    def create_accumulator(
        self,
    ):
        return (
            0,
            0.0,
            0.0,
        )

    def add(
        self,
        value,
        accumulator,
    ):
        count = (
            accumulator[0]
            + 1
        )

        amount = float(
            value.get(
                "amount",
                0,
            )
        )

        total_amount = (
            accumulator[1]
            + amount
        )

        max_amount = max(
            accumulator[2],
            amount,
        )

        return (
            count,
            total_amount,
            max_amount,
        )

    def get_result(
        self,
        accumulator,
    ):
        return accumulator

    def merge(
        self,
        first,
        second,
    ):
        return (
            first[0]
            + second[0],
            first[1]
            + second[1],
            max(
                first[2],
                second[2],
            ),
        )


class CustomerWindowProcess(
    ProcessWindowFunction
):

    def process(
        self,
        key,
        context,
        aggregates,
    ):
        aggregate = next(
            iter(
                aggregates
            )
        )

        transaction_count = int(
            aggregate[0]
        )

        total_amount = float(
            aggregate[1]
        )

        max_amount = float(
            aggregate[2]
        )

        is_velocity_fraud = (
            transaction_count >= 5
            or total_amount >= 10000
        )

        window_start = (
            clickhouse_datetime(
                context.window().start
            )
        )

        window_end = (
            clickhouse_datetime(
                context.window().end
            )
        )

        yield {
            "customer_id": str(
                key
            ),
            "window_start":
                window_start,
            "window_end":
                window_end,
            "transaction_count":
                transaction_count,
            "total_amount":
                total_amount,
            "max_amount":
                max_amount,
            "is_velocity_fraud":
                is_velocity_fraud,
            "processed_at":
                clickhouse_datetime(),
        }


def clickhouse_request(
    query,
    payload=None,
):
    parameters = urlencode(
        {
            "database":
                CLICKHOUSE_DATABASE,
            "query":
                query,
        }
    )

    url = (
        f"http://"
        f"{CLICKHOUSE_HOST}:"
        f"{CLICKHOUSE_PORT}/?"
        f"{parameters}"
    )

    credentials = (
        f"{CLICKHOUSE_USER}:"
        f"{CLICKHOUSE_PASSWORD}"
    )

    encoded_credentials = (
        base64.b64encode(
            credentials.encode(
                "utf-8"
            )
        ).decode(
            "ascii"
        )
    )

    body = None

    if payload is not None:
        body = payload.encode(
            "utf-8"
        )

    request = Request(
        url=url,
        data=body,
        method="POST",
        headers={
            "Authorization":
                "Basic "
                + encoded_credentials,
            "Content-Type":
                "application/json",
        },
    )

    with urlopen(
        request,
        timeout=10,
    ) as response:
        return (
            response
            .read()
            .decode(
                "utf-8"
            )
        )


class ProcessedTransactionWriter(
    MapFunction
):

    def map(
        self,
        transaction,
    ):
        row = {
            "event_time":
                transaction.get(
                    "event_time"
                ),

            "transaction_id":
                str(
                    transaction.get(
                        "transaction_id",
                        "",
                    )
                ),

            "customer_id":
                str(
                    transaction.get(
                        "customer_id",
                        "",
                    )
                ),

            "account_id":
                str(
                    transaction.get(
                        "account_id",
                        "",
                    )
                ),

            "merchant":
                str(
                    transaction.get(
                        "merchant",
                        "",
                    )
                ),

            "category":
                str(
                    transaction.get(
                        "category",
                        "",
                    )
                ),

            "amount":
                round(
                    float(
                        transaction.get(
                            "amount",
                            0,
                        )
                    ),
                    2,
                ),

            "currency":
                str(
                    transaction.get(
                        "currency",
                        "",
                    )
                ),

            "country":
                str(
                    transaction.get(
                        "country",
                        "",
                    )
                ),

            "city":
                str(
                    transaction.get(
                        "city",
                        "",
                    )
                ),

            "device_id":
                str(
                    transaction.get(
                        "device_id",
                        "",
                    )
                ),

            "payment_method":
                str(
                    transaction.get(
                        "payment_method",
                        "",
                    )
                ),

            "is_international":
                int(
                    bool(
                        transaction.get(
                            "is_international",
                            False,
                        )
                    )
                ),

            "validation_status":
                str(
                    transaction.get(
                        "validation_status",
                        "INVALID",
                    )
                ),

            "is_fraud":
                int(
                    bool(
                        transaction.get(
                            "is_fraud",
                            False,
                        )
                    )
                ),

            "fraud_score":
                int(
                    transaction.get(
                        "fraud_score",
                        0,
                    )
                ),

            "fraud_reasons":
                list(
                    transaction.get(
                        "fraud_reasons",
                        [],
                    )
                ),

            "ingested_at":
                clickhouse_datetime(),
        }

        query = """
INSERT INTO processed_transactions
(
    event_time,
    transaction_id,
    customer_id,
    account_id,
    merchant,
    category,
    amount,
    currency,
    country,
    city,
    device_id,
    payment_method,
    is_international,
    validation_status,
    is_fraud,
    fraud_score,
    fraud_reasons,
    ingested_at
)
FORMAT JSONEachRow
""".strip()

        try:
            clickhouse_request(
                query,
                json.dumps(
                    row
                ),
            )

        except Exception as exc:
            print(
                "ClickHouse processed "
                "transaction insert error:",
                str(exc),
            )

            raise

        return transaction


class WindowMetricsWriter(
    MapFunction
):

    def map(
        self,
        metric,
    ):
        row = {
            "customer_id":
                str(
                    metric.get(
                        "customer_id",
                        "",
                    )
                ),

            "window_start":
                metric.get(
                    "window_start"
                ),

            "window_end":
                metric.get(
                    "window_end"
                ),

            "transaction_count":
                int(
                    metric.get(
                        "transaction_count",
                        0,
                    )
                ),

            "total_amount":
                round(
                    float(
                        metric.get(
                            "total_amount",
                            0,
                        )
                    ),
                    2,
                ),

            "max_amount":
                round(
                    float(
                        metric.get(
                            "max_amount",
                            0,
                        )
                    ),
                    2,
                ),

            "is_velocity_fraud":
                int(
                    bool(
                        metric.get(
                            "is_velocity_fraud",
                            False,
                        )
                    )
                ),

            "processed_at":
                metric.get(
                    "processed_at",
                    clickhouse_datetime(),
                ),
        }

        query = """
INSERT INTO customer_window_metrics
(
    customer_id,
    window_start,
    window_end,
    transaction_count,
    total_amount,
    max_amount,
    is_velocity_fraud,
    processed_at
)
FORMAT JSONEachRow
""".strip()

        try:
            clickhouse_request(
                query,
                json.dumps(
                    row
                ),
            )

        except Exception as exc:
            print(
                "ClickHouse window "
                "metrics insert error:",
                str(exc),
            )

            raise

        return metric


def main():
    env = (
        StreamExecutionEnvironment
        .get_execution_environment()
    )

    env.set_parallelism(
        2
    )

    env.enable_checkpointing(
        30000,
        CheckpointingMode.EXACTLY_ONCE,
    )

    checkpoint_config = (
        env.get_checkpoint_config()
    )

    checkpoint_config.set_checkpoint_timeout(
        60000
    )

    checkpoint_config.set_min_pause_between_checkpoints(
        10000
    )

    checkpoint_config.set_max_concurrent_checkpoints(
        1
    )

    checkpoint_config.set_externalized_checkpoint_retention(
        ExternalizedCheckpointRetention
        .RETAIN_ON_CANCELLATION
    )

    kafka_source = (
        KafkaSource
        .builder()
        .set_bootstrap_servers(
            KAFKA_BOOTSTRAP_SERVERS
        )
        .set_topics(
            KAFKA_TOPIC
        )
        .set_group_id(
            KAFKA_GROUP_ID
        )
        .set_starting_offsets(
            KafkaOffsetsInitializer
            .committed_offsets(
                KafkaOffsetResetStrategy
                .EARLIEST
            )
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    raw_transactions = (
        env.from_source(
            kafka_source,
            WatermarkStrategy
            .no_watermarks(),
            "Kafka Transactions Source",
        )
    )

    validated_transactions = (
        raw_transactions
        .map(
            ValidateTransactionMap()
        )
        .filter(
            lambda transaction:
                transaction
                is not None
        )
        .name(
            "Validate Transactions"
        )
    )

    fraud_transactions = (
        validated_transactions
        .map(
            FraudDetectionMap()
        )
        .name(
            "Fraud Detection"
        )
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(
            Duration.of_seconds(
                5
            )
        )
        .with_timestamp_assigner(
            EventTimestampAssigner()
        )
    )

    timestamped_transactions = (
        fraud_transactions
        .assign_timestamps_and_watermarks(
            watermark_strategy
        )
        .name(
            "Event Time and Watermarks"
        )
    )

    processed_stream = (
        timestamped_transactions
        .map(
            ProcessedTransactionWriter()
        )
        .name(
            "ClickHouse Processed "
            "Transactions"
        )
    )

    processed_stream.print(
        "processed_transaction"
    )

    valid_transactions = (
        timestamped_transactions
        .filter(
            lambda transaction:
                transaction.get(
                    "validation_status"
                )
                == "VALID"
        )
        .name(
            "Valid Transactions"
        )
    )

    customer_window_metrics = (
        valid_transactions
        .key_by(
            lambda transaction:
                str(
                    transaction.get(
                        "customer_id",
                        "",
                    )
                )
        )
        .window(
            TumblingEventTimeWindows
            .of(
                Time.minutes(
                    1
                )
            )
        )
        .aggregate(
            CustomerWindowAggregate(),
            CustomerWindowProcess(),
        )
        .name(
            "Customer Tumbling Window"
        )
    )

    metrics_stream = (
        customer_window_metrics
        .map(
            WindowMetricsWriter()
        )
        .name(
            "ClickHouse Customer "
            "Window Metrics"
        )
    )

    metrics_stream.print(
        "customer_window_metric"
    )

    env.execute(
        "Fraud Detection ClickHouse Pipeline"
    )


if __name__ == "__main__":
    main()