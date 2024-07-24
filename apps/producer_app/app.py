import random
from datetime import datetime, timedelta

from confluent_kafka import Producer
import json

conf = {'bootstrap.servers': "redpanda:9092"}


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def random_transaction_id():
    return random.randint(0, 999999)


def random_store_id():
    return random.randint(0, 100)


def random_product_id():
    return random.randint(0, 1000)


def random_quantity():
    return random.randint(0, 5)


def random_price():
    return round(random.uniform(0, 100), 2)


def random_datetime(min_year=2023, max_year=datetime.now().year):
    # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
    start = datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    return start + (end - start) * random.random()


def sales_data():
    return {
        "transaction_id": random_transaction_id(),
        "store_id": random_store_id(),
        "product_id": random_product_id(),
        "quantity": random_quantity(),
        "price": random_price(),
        "timestamp": random_datetime().strftime("%Y-%m-%dT%H:%M:%SZ")
    }


if __name__ == "__main__":
    topic = 'retail-sales'
    producer = Producer(**conf)

    for _ in range(20000):
        producer.produce(topic, json.dumps(sales_data()).encode('utf-8'), callback=delivery_report)


    producer.flush()
