import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

fake = Faker()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TRANSACTION_TOPIC = os.getenv(
    "KAFKA_TRANSACTION_TOPIC",
    "transactions",
)

producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "fraud-transaction-producer",
        "acks": "all",
    }
)

merchants = [
    ("Amazon", "Ecommerce"),
    ("Walmart", "Grocery"),
    ("Target", "Retail"),
    ("Uber", "Transport"),
    ("Netflix", "Entertainment"),
    ("Starbucks", "Food"),
    ("Apple", "Electronics"),
    ("Best Buy", "Electronics"),
    ("Airbnb", "Travel"),
    ("Shell", "Fuel"),
]

payment_methods = [
    "credit_card",
    "debit_card",
    "digital_wallet",
    "bank_transfer",
]

countries = [
    ("US", "New York"),
    ("US", "Chicago"),
    ("US", "Dallas"),
    ("US", "Los Angeles"),
    ("US", "Seattle"),
    ("CA", "Toronto"),
    ("GB", "London"),
    ("IN", "Mumbai"),
    ("DE", "Berlin"),
]


def generate_transaction():
    merchant, category = random.choice(merchants)
    country, city = random.choice(countries)

    customer_number = random.randint(1, 100)
    customer_id = f"cust_{customer_number:04d}"

    normal_amount = round(random.uniform(5, 500), 2)

    if random.random() < 0.05:
        amount = round(random.uniform(2000, 10000), 2)
    else:
        amount = normal_amount

    transaction = {
        "transaction_id": f"tx_{uuid.uuid4().hex}",
        "customer_id": customer_id,
        "account_id": f"acc_{random.randint(1, 200):04d}",
        "merchant": merchant,
        "category": category,
        "amount": amount,
        "currency": "USD",
        "country": country,
        "city": city,
        "device_id": f"device_{random.randint(1, 150):04d}",
        "payment_method": random.choice(payment_methods),
        "is_international": country != "US",
        "ip_address": fake.ipv4_public(),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }

    return transaction


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
        return

    print(
        f"Delivered | "
        f"topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def main():
    print("Starting transaction producer")
    print(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TRANSACTION_TOPIC}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            transaction = generate_transaction()

            customer_id = transaction["customer_id"]

            producer.produce(
                topic=KAFKA_TRANSACTION_TOPIC,
                key=customer_id.encode("utf-8"),
                value=json.dumps(transaction).encode("utf-8"),
                callback=delivery_report,
            )

            producer.poll(0)

            print(json.dumps(transaction, indent=2))

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        remaining = producer.flush(10)

        if remaining == 0:
            print("All pending messages delivered.")
        else:
            print(f"{remaining} messages were not delivered.")


if __name__ == "__main__":
    main()