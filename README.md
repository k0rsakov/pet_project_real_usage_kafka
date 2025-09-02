# Примеры использования Kafka

Kafka верхнеуровнево:

```mermaid
flowchart LR
    A["Producer<br>(Отправитель)"] -- Отправляет сообщения --> B((Kafka<br>Topic))
    B -- Сообщения поступают --> C["Consumer 1<br>(Получатель 1)"]
    B -- Сообщения поступают --> D["Consumer 2<br>(Получатель 2)"]
    B -- Сообщения поступают --> F["Consumer N<br>(Получатель N)"]

    style A fill:#f9f,stroke:#333,stroke-width:1px
    style B fill:#bbf,stroke:#333,stroke-width:2px
    style C fill:#afa,stroke:#333,stroke-width:1px
    style D fill:#afa,stroke:#333,stroke-width:1px
    style F fill:#aff,stroke:#333,stroke-width:1px
```

Kafka + ClickHouse верхнеуровнево:

```mermaid
flowchart TB
    A["Producer<br>(Отправитель)"] -- Отправляет сообщения --> B((Kafka<br>Topic))
    B -- Сообщения поступают --> C["Таблица с движком Kafka<br>(consumer, логический)"]
    C -- SELECT * FROM (Kafka) --> D["Материализованное<br>представление"]
    D -- INSERT INTO --> E["Физическая таблица<br>(хранение данных)"]

    style A fill:#f9f,stroke:#333,stroke-width:1px
    style B fill:#bbf,stroke:#333,stroke-width:2px
    style C fill:#ffe599,stroke:#333,stroke-width:2px
    style D fill:#b6d7a8,stroke:#333,stroke-width:2px
    style E fill:#76a5af,stroke:#333,stroke-width:2px
```

Kafka + Python + MinIO верхнеуровнево:

```mermaid
flowchart TB
    A["Kafka<br>Topic"] -- poll messages --> B["Python Consumer<br>(kafka_to_minio_parquet_on_python.py)"]
    B -- batch (.json -> pd.DataFrame) --> C["Pandas DataFrame"]
    C -- Save as .parquet --> D["MinIO<br>(S3 совместимое хранилище)"]

    style A fill:#bbf,stroke:#333,stroke-width:2px
    style B fill:#ffe599,stroke:#333,stroke-width:2px
    style C fill:#b6d7a8,stroke:#333,stroke-width:2px
    style D fill:#76a5af,stroke:#333,stroke-width:2px
```




## Команды и скрипты

### Создание виртуального окружения

```bash
python3.12 -m venv venv && \
source venv/bin/activate && \
pip install --upgrade pip && \
pip install poetry && \
poetry lock && \
poetry install
```

#### Добавление новых зависимостей в окружение

```bash
poetry lock && \
poetry install
```

### Поднятие инфраструктуры

```bash
docker compose up -d
```

Если отображаются исключения, то необходимо выполнить команду ниже, так как в проекте используется своя сборка Airflow:

```bash
docker compose build
```

### Подключение к Minio

Параметры подключения стандартные:

- `login`: `minioadmin`
- `password`: `minioadmin`

## TODO

- [ ] Описать получение Kafka CLI
- [ ] Команды чтения

```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group ParquetConsumerGroup --describe
```

```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group mygroup --describe
```

Очистка offset:
```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group ParquetConsumerGroup --to-earliest --reset-offsets --execute --topic music_events
```


```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group mygroup --to-earliest --reset-offsets --execute --topic music_events
```