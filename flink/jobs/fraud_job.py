import base64
import json
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pyflink.common import Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    MapFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import TumblingEventTimeWindows


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


def validate_transaction(raw_message):
    try:
        transaction = json.loads(raw_message)

        missing_fields = [
            field
            for field in REQUIRED_FIELDS
            if field not in transaction or transaction[field] is None
        ]

        if missing_fields:
            return None

        amount = float(transaction["amount"])

        if amount <= 0:
            return None

        transaction["amount"] = amount
        transaction["validation_status"] = "VALID"

        return json.dumps(transaction)

    except (json.JSONDecodeError, TypeError, ValueError):
        return None


class TransactionTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        transaction = json.loads(value)

        event_time = datetime.fromisoformat(
            transaction["event_time"].replace("Z", "+00:00")
        )

        return int(event_time.timestamp() * 1000)


def apply_fraud_rules(transaction_json):
    transaction = json.loads(transaction_json)

    amount = float(transaction["amount"])
    is_international = bool(transaction.get("is_international", False))

    fraud_reasons = []

    if amount >= 2000:
        fraud_reasons.append("HIGH_VALUE_TRANSACTION")

    if is_international and amount >= 1000:
        fraud_reasons.append("HIGH_VALUE_INTERNATIONAL")

    if amount >= 5000:
        fraud_reasons.append("VERY_HIGH_VALUE_TRANSACTION")

    transaction["is_fraud"] = len(fraud_reasons) > 0
    transaction["fraud_reasons"] = fraud_reasons
    transaction["fraud_score"] = len(fraud_reasons)

    return json.dumps(transaction)


def clickhouse_insert(table_name, row):
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = os.getenv("CLICKHOUSE_PORT", "8123")
    database = os.getenv("CLICKHOUSE_DATABASE", "fraud_detection")
    user = os.getenv("CLICKHOUSE_USER", "fraud_user")
    password = os.getenv("CLICKHOUSE_PASSWORD", "change_me")

    query = f"INSERT INTO {database}.{table_name} FORMAT JSONEachRow"

    params = urlencode(
        {
            "query": query,
            "async_insert": "1",
            "wait_for_async_insert": "1",
            "date_time_input_format": "best_effort",
        }
    )

    url = f"http://{host}:{port}/?{params}"

    credentials = base64.b64encode(
        f"{user}:{password}".encode("utf-8")
    ).decode("ascii")

    body = (
        json.dumps(row, separators=(",", ":")) + "\n"
    ).encode("utf-8")

    headers = {
        "Authorization": f"Basic {credentials}",
        "Content-Type": "application/json",
    }

    for attempt in range(1, 4):
        try:
            request = Request(
                url=url,
                data=body,
                headers=headers,
                method="POST",
            )

            with urlopen(request, timeout=10) as response:
                if response.status != 200:
                    raise RuntimeError(
                        f"ClickHouse HTTP status {response.status}"
                    )

            return

        except Exception:
            if attempt == 3:
                raise

            time.sleep(attempt)


class ClickHouseTransactionWriter(MapFunction):
    def map(self, transaction_json):
        transaction = json.loads(transaction_json)

        row = {
            "event_time": transaction["event_time"],
            "transaction_id": transaction["transaction_id"],
            "customer_id": transaction["customer_id"],
            "account_id": transaction["account_id"],
            "merchant": transaction["merchant"],
            "category": transaction["category"],
            "amount": float(transaction["amount"]),
            "currency": transaction["currency"],
            "country": transaction["country"],
            "city": transaction["city"],
            "device_id": transaction["device_id"],
            "payment_method": transaction["payment_method"],
            "is_international": (
                1 if transaction.get("is_international", False) else 0
            ),
            "validation_status": transaction["validation_status"],
            "is_fraud": 1 if transaction["is_fraud"] else 0,
            "fraud_score": int(transaction["fraud_score"]),
            "fraud_reasons": transaction["fraud_reasons"],
        }

        clickhouse_insert(
            "processed_transactions",
            row,
        )

        return (
            f"CLICKHOUSE_TX_OK | "
            f"{transaction['transaction_id']}"
        )


class CustomerWindowAggregate(AggregateFunction):
    def create_accumulator(self):
        return 0, 0.0, 0.0

    def add(self, value, accumulator):
        transaction = json.loads(value)
        amount = float(transaction["amount"])

        return (
            accumulator[0] + 1,
            accumulator[1] + amount,
            max(accumulator[2], amount),
        )

    def get_result(self, accumulator):
        return accumulator

    def merge(self, accumulator_a, accumulator_b):
        return (
            accumulator_a[0] + accumulator_b[0],
            accumulator_a[1] + accumulator_b[1],
            max(accumulator_a[2], accumulator_b[2]),
        )


class CustomerWindowResult(ProcessWindowFunction):
    def process(self, key, context, aggregates):
        count, total_amount, max_amount = next(iter(aggregates))

        window_start = datetime.fromtimestamp(
            context.window().start / 1000,
            tz=timezone.utc,
        ).isoformat()

        window_end = datetime.fromtimestamp(
            context.window().end / 1000,
            tz=timezone.utc,
        ).isoformat()

        is_velocity_fraud = (
            count >= 5
            or total_amount >= 3000
        )

        yield json.dumps(
            {
                "customer_id": key,
                "window_start": window_start,
                "window_end": window_end,
                "transaction_count": count,
                "total_amount": round(total_amount, 2),
                "max_amount": round(max_amount, 2),
                "is_velocity_fraud": is_velocity_fraud,
            }
        )


class ClickHouseWindowWriter(MapFunction):
    def map(self, result_json):
        result = json.loads(result_json)

        row = {
            "customer_id": result["customer_id"],
            "window_start": result["window_start"],
            "window_end": result["window_end"],
            "transaction_count": int(
                result["transaction_count"]
            ),
            "total_amount": float(
                result["total_amount"]
            ),
            "max_amount": float(
                result["max_amount"]
            ),
            "is_velocity_fraud": (
                1 if result["is_velocity_fraud"] else 0
            ),
        }

        clickhouse_insert(
            "customer_window_metrics",
            row,
        )

        return (
            f"CLICKHOUSE_WINDOW_OK | "
            f"{result['customer_id']} | "
            f"{result['window_start']}"
        )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(2)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_topics("transactions")
        .set_group_id("fraud-clickhouse-consumer-v1")
        .set_starting_offsets(
            KafkaOffsetsInitializer.latest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    raw_transactions = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Transactions Source",
    )

    validated_transactions = (
        raw_transactions
        .map(
            validate_transaction,
            output_type=Types.STRING(),
        )
        .filter(
            lambda transaction: transaction is not None
        )
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        )
        .with_timestamp_assigner(
            TransactionTimestampAssigner()
        )
        .with_idleness(
            Duration.of_seconds(10)
        )
    )

    event_time_transactions = (
        validated_transactions
        .assign_timestamps_and_watermarks(
            watermark_strategy
        )
    )

    scored_transactions = (
        event_time_transactions
        .map(
            apply_fraud_rules,
            output_type=Types.STRING(),
        )
    )

    scored_transactions.map(
        ClickHouseTransactionWriter(),
        output_type=Types.STRING(),
    ).print()

    scored_transactions.filter(
        lambda transaction:
            json.loads(transaction)["is_fraud"]
    ).map(
        lambda transaction:
            f"FRAUD_ALERT | {transaction}",
        output_type=Types.STRING(),
    ).print()

    customer_keyed_stream = (
        scored_transactions
        .key_by(
            lambda transaction:
                json.loads(transaction)["customer_id"],
            key_type=Types.STRING(),
        )
    )

    customer_windows = (
        customer_keyed_stream
        .window(
            TumblingEventTimeWindows.of(
                Time.seconds(30)
            )
        )
        .aggregate(
            CustomerWindowAggregate(),
            CustomerWindowResult(),
            accumulator_type=Types.TUPLE(
                [
                    Types.INT(),
                    Types.DOUBLE(),
                    Types.DOUBLE(),
                ]
            ),
            output_type=Types.STRING(),
        )
    )

    customer_windows.map(
        ClickHouseWindowWriter(),
        output_type=Types.STRING(),
    ).print()

    customer_windows.filter(
        lambda result:
            json.loads(result)["is_velocity_fraud"]
    ).map(
        lambda result:
            f"VELOCITY_ALERT | {result}",
        output_type=Types.STRING(),
    ).print()

    env.execute(
        "Fraud Detection ClickHouse Pipeline"
    )


if __name__ == "__main__":
    main()