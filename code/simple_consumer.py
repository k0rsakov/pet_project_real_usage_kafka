from confluent_kafka import Consumer, KafkaError, TopicPartition


def consume_messages(topic: str | None = None, offset: int | None = None) -> None:
    conf = {
        "bootstrap.servers": "localhost:19092",
        "group.id": "mygroup",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(conf)

    if offset is not None:
        partitions = consumer.list_topics(topic).topics[topic].partitions
        for partition in partitions:
            consumer.assign([TopicPartition(topic, partition, offset)])
    else:
        consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError:
                    print("Reached end of partition")
                else:
                    print(f"Error: {msg.error()}")
            else:
                print(f"Received message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


# Читать с начала
consume_messages("music_events")

# Читать с определенного offset
# consume_messages(topic='my_topic', offset=5)  # noqa: ERA001
