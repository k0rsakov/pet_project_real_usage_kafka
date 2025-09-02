import json
import uuid
from datetime import UTC, datetime

import pandas as pd
from confluent_kafka import Consumer, TopicPartition
from cred import access_key, secret_key

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
KAFKA_TOPIC = "music_events"
KAFKA_GROUP = "ParquetConsumerGroup"
BATCH_SIZE = 100

bucket_name = "prod-python"

storage_options = {
    "key": access_key,
    "secret": secret_key,
    "client_kwargs": {"endpoint_url": "http://localhost:9000"},
}

def save_batch_to_minio(batch) -> None:
    """
    Метод для сохранения батча сообщений в MinIO в формате Parquet.

    :param batch: Список сообщений для сохранения.
    :return: None
    """
    if not batch:
        return
    df = pd.json_normalize(batch)
    date_str = datetime.now(UTC).strftime("%Y-%m-%d")
    file_uuid = uuid.uuid4()
    path = f"s3://{bucket_name}/{date_str}/{file_uuid}.parquet"
    df.to_parquet(
        path=path,
        index=False,
        storage_options=storage_options,
    )
    print(f"Batch saved to {path}")

def consume_messages(
        topic: str | None= None,
        batch_size: int = 100,
        offset: int | None = None,
) -> None:
    """
    Метод для чтения сообщений из Kafka и сохранения их в MinIO в формате Parquet.

    :param topic: Топик Kafka для чтения сообщений.
    :param batch_size: Размер батча для сохранения в MinIO.
    :param offset: Offset для начала чтения сообщений (если None, читаем с текущего положения).
    :return: None
    """
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)

    if offset is not None:
        partitions = consumer.list_topics(topic).topics[topic].partitions
        for partition in partitions:
            consumer.assign([TopicPartition(topic, partition, offset)])
    else:
        consumer.subscribe([topic])

    batch = []
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"Bad message: {e}")
                continue
            batch.append(event)
            if len(batch) >= batch_size:
                save_batch_to_minio(batch)
                batch = []
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        if batch:
            save_batch_to_minio(batch)
        consumer.close()

if __name__ == "__main__":
    # Пример вызова: читаем с начала топика
    consume_messages(topic=KAFKA_TOPIC, batch_size=BATCH_SIZE)
