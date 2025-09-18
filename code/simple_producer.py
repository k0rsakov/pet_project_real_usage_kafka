import json
import time

import pendulum
import uuid_utils as uuid
from confluent_kafka import Producer
from faker import Faker


def generate_list_of_dict() -> dict[str, str]:
    fake = Faker(locale="ru_RU")

    return {
        "uuid": str(uuid.uuid7()),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "middle_name": fake.middle_name(),
        "timestamp": pendulum.now("UTC").to_iso8601_string(),
    }


# Define the Kafka configuration
conf = {"bootstrap.servers": "localhost:19092"}

# Create a Producer instance with the above configuration
producer = Producer(conf)

# simple RPS
interval = 1.0 / 1

while True:
    start = time.perf_counter()
    # Define some data to send to Kafka
    data = generate_list_of_dict()

    # Convert the data to a JSON string
    data_str = json.dumps(data)
    print(f"Event sent: {data_str}\n\n")

    # Produce a message to the "my_topic" topic
    producer.produce(topic="my_topic", value=data_str)

    # Flush the producer to ensure all messages are sent
    producer.flush()

    # Sleep for a second before producing the next set of messages

    elapsed = time.perf_counter() - start
    sleep_time = max(0, interval - elapsed)
    time.sleep(sleep_time)
