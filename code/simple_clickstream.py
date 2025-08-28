import json
import random
import time

import pandas as pd
import pendulum
from confluent_kafka import Producer
from faker import Faker


# 1. Генерация DataFrame с пользователями
def generate_users_df(n=100):
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

# 2. Генерация событий
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

fake = Faker(locale="ru_RU")

def event_track_playback(user):
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 1,
            "event_type": events[1],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track": fake.uuid4(),
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_pause_track(user):
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 2,
            "event_type": events[2],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track": fake.uuid4(),
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_resume_track(user):
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 3,
            "event_type": events[3],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track": fake.uuid4(),
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_skipping_track_next(user):
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 4,
            "event_type": events[4],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track_prev": fake.uuid4(),
            "uuid_track_current": fake.uuid4(),
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_skipping_track_prev(user):
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 5,
            "event_type": events[5],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track_current": fake.uuid4(),
            "uuid_track_prev": fake.uuid4(),
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_adding_track_to_playlist(user):
    now = pendulum.now()
    return {
        "event_params": {
            "event_type_id": 6,
            "event_type": events[6],
            "user_id": user["id"],
            "platform_token": user["platform_token"],
            "ipv4": user["ipv4"],
            "country": user["country"],
            "uuid_track": fake.uuid4(),
            "uuid_playlist": fake.uuid4(),
        },
        "event_timestamp_ms": {
            "ts": now.int_timestamp,
            "ts_ms": int(now.float_timestamp * 1000),
        },
    }

def event_track_like(user):
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

def event_track_unlike(user):
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

def generate_realistic_event(df):
    event_ids = list(events.keys())
    event_weights = [0.35, 0.15, 0.13, 0.12, 0.07, 0.08, 0.06, 0.04]  # ваши вероятности
    user_row = df.sample(n=1).iloc[0]
    event_type_id = random.choices(population=event_ids, weights=event_weights, k=1)[0]  # noqa: S311
    event_func = event_functions[event_type_id]
    return event_func(user_row)

# 3. Отправка событий в Kafka
def send_clickstream_events_to_kafka_rps(
        df: pd.DataFrame=None,
        topic="music_events",
        bootstrap_servers="localhost:19092",
        rps=2,
):
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

df = generate_users_df(100)

send_clickstream_events_to_kafka_rps(df=df, topic="music_events", bootstrap_servers="localhost:19092", rps=2)
