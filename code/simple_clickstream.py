import json
import random
import time
from typing import Any

import pandas as pd
import pendulum
from confluent_kafka import Producer
from faker import Faker

events = {
    1: "track_playback",
    2: "Pause_track",
    3: "resume_track",
    4: "skipping_track_next",
    5: "skipping_track_prev",
    6: "adding_track_to_playlist",
    7: "track_like",
    8: "track_unlike",
}

FAKER = Faker(locale="ru_RU")


def generate_users_df(n: int = 100) -> pd.DataFrame:
    """
    Метод генерирует DataFrame с информацией о пользователях.

    :param n: Количество пользователей для генерации.
    :return: DataFrame с информацией о пользователях.
    """
    faker = Faker(locale="ru_RU")
    users = []
    for _ in range(n):
        users.append({  # noqa: PERF401
            "id": faker.uuid4(),
            "platform_token": faker.android_platform_token(),
            "ipv4": faker.ipv4(),
            "country": faker.country(),
        })
    return pd.DataFrame(users)



def event_track_playback(user: pd.Series = None) -> dict[str, Any]:
    """
    Метод возвращает событие воспроизведения трека.

    :param user: Словарь с информацией о пользователе.
    :return: Словарь, представляющий событие воспроизведения трека.
    """
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 1,
            "event_type": events[1],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track": FAKER.uuid4(),
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_pause_track(user: pd.Series = None) -> dict[str, Any]:
    """
    Метод возвращает событие паузы трека.

    :param user: Словарь с информацией о пользователе.
    :return: Словарь, представляющий событие паузы трека.
    """
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 2,
            "event_type": events[2],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track": FAKER.uuid4(),
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_resume_track(user: pd.Series = None) -> dict[str, Any]:
    """
    Метод возвращает событие возобновления трека.

    :param user: Словарь с информацией о пользователе.
    :return: Словарь, представляющий событие возобновления трека.
    """
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 3,
            "event_type": events[3],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track": FAKER.uuid4(),
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_skipping_track_next(user: pd.Series = None) -> dict[str, Any]:
    """
    Метод возвращает событие пропуска трека вперед.

    :param user: Словарь с информацией о пользователе.
    :return: Словарь, представляющий событие пропуска трека вперед.
    """
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 4,
            "event_type": events[4],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track_prev": FAKER.uuid4(),
            "uuid_track_current": FAKER.uuid4(),
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_skipping_track_prev(user: pd.Series = None) -> dict[str, Any]:
    """
    Метод возвращает событие пропуска трека назад.

    :param user: Словарь с информацией о пользователе.
    :return: Словарь, представляющий событие пропуска трека назад.
    """
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 5,
            "event_type": events[5],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track_current": FAKER.uuid4(),
            "uuid_track_prev": FAKER.uuid4(),
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_adding_track_to_playlist(user: pd.Series = None) -> dict[str, Any]:
    """
    Метод возвращает событие добавления трека в плейлист.

    :param user: Словарь с информацией о пользователе.
    :return: Словарь, представляющий событие добавления трека в плейлист.
    """
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 6,
            "event_type": events[6],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track": FAKER.uuid4(),
            "uuid_playlist": FAKER.uuid4(),
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_track_like(user: pd.Series = None) -> dict[str, Any]:
    """
    Метод возвращает событие лайка трека.

    :param user: Словарь с информацией о пользователе.
    :return: Словарь, представляющий событие лайка трека.
    """
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 7,
            "event_type": events[7],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track_like_status": 1,
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_track_unlike(user: pd.Series = None) -> dict[str, Any]:
    """
    Метод возвращает событие снятия лайка с трека.

    :param user: Словарь с информацией о пользователе.
    :return: Словарь, представляющий событие снятия лайка с трека.
    """
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 8,
            "event_type": events[8],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track_like_status": 0,
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

event_functions = {
    1: event_track_playback,
    2: event_pause_track,
    3: event_resume_track,
    4: event_skipping_track_next,
    5: event_skipping_track_prev,
    6: event_adding_track_to_playlist,
    7: event_track_like,
    8: event_track_unlike,
}

def generate_realistic_event(df: pd.DataFrame = None) -> dict[str, Any]:
    """
    Метод генерирует реалистичное событие на основе DataFrame пользователей.

    :param df: DataFrame с информацией о пользователях.
    :return: Словарь, представляющий сгенерированное событие.
    """
    event_ids = list(events.keys())
    # вероятности того или иного события
    event_weights = [0.35, 0.15, 0.13, 0.12, 0.07, 0.08, 0.06, 0.04]
    user_row = df.sample(n=1).iloc[0]
    print(type(user_row))
    event_type_id = random.choices(
        population=event_ids,
        weights=event_weights, k=1
    )[0]  # noqa: S311
    event_func = event_functions[event_type_id]
    return event_func(user_row)

def send_clickstream_events_to_kafka_rps(
        df: pd.DataFrame = None,
        topic: str = "music_events",
        bootstrap_servers: str = "localhost:19092",
        rps: int = 2,
) -> None:
    """
    Kafka producer, который отправляет события в Kafka с заданным RPS.

    :param df: DataFrame с информацией о пользователях.
    :param topic: Название топика Kafka.
    :param bootstrap_servers: Адреса bootstrap серверов Kafka.
    :param rps: Количество событий в секунду.
    :return: None
    """
    conf = {"bootstrap.servers": bootstrap_servers}
    producer = Producer(conf)
    interval = 1.0 / rps

    try:
        while True:
            start = time.perf_counter()
            event = generate_realistic_event(df)
            data_str = json.dumps(event, ensure_ascii=False)
            print(f"Event sent: {data_str}\n")
            producer.produce(topic=topic, value=data_str)
            producer.flush()
            elapsed = time.perf_counter() - start
            sleep_time = max(0, interval - elapsed)
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        print("Stopped by user.")

if __name__ == "__main__":
    df = generate_users_df(n=10)
    send_clickstream_events_to_kafka_rps(df, topic="music_events", bootstrap_servers="localhost:19092", rps=2)
