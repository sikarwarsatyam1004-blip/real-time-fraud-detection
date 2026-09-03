import json
from datetime import datetime, timezone

from pyflink.common import Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
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


class CustomerWindowAggregate(AggregateFunction):
    def create_accumulator(self):
        return 0, 0.0, 0.0

    def add(self, value, accumulator):
        transaction = json.loads(value)
        amount = float(transaction["amount"])

        count = accumulator[0] + 1
        total_amount = accumulator[1] + amount
        max_amount = max(accumulator[2], amount)

        return count, total_amount, max_amount

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

        result = {
            "customer_id": key,
            "window_start": window_start,
            "window_end": window_end,
            "transaction_count": count,
            "total_amount": round(total_amount, 2),
            "max_amount": round(max_amount, 2),
            "is_velocity_fraud": is_velocity_fraud,
        }

        yield json.dumps(result)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(2)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_topics("transactions")
        .set_group_id("fraud-window-metadata-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
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
        .filter(lambda transaction: transaction is not None)
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(TransactionTimestampAssigner())
        .with_idleness(Duration.of_seconds(10))
    )

    event_time_transactions = (
        validated_transactions
        .assign_timestamps_and_watermarks(watermark_strategy)
    )

    customer_keyed_stream = event_time_transactions.key_by(
        lambda transaction: json.loads(transaction)["customer_id"],
        key_type=Types.STRING(),
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

    velocity_alerts = customer_windows.filter(
        lambda result: json.loads(result)["is_velocity_fraud"]
    )

    normal_windows = customer_windows.filter(
        lambda result: not json.loads(result)["is_velocity_fraud"]
    )

    velocity_alerts.map(
        lambda result: f"VELOCITY_ALERT | {result}",
        output_type=Types.STRING(),
    ).print()

    normal_windows.map(
        lambda result: f"WINDOW_NORMAL | {result}",
        output_type=Types.STRING(),
    ).print()

    env.execute("Fraud Detection Window Metadata")


if __name__ == "__main__":
    main()