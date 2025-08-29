from cred import access_key, secret_key
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType

minio_endpoint = "http://localhost:9000"
bucket_name = "prod-spark"

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
KAFKA_TOPIC = "music_events"

# Определяем только верхнеуровневую структуру
event_params_schema = StructType([])
event_timestamp_schema = StructType([])

message_schema = StructType([
    StructField(name="event_params", dataType=StringType()),  # Можно StringType, если не хотим парсить глубже
    StructField(name="event_timestamp_ms", dataType=StringType()),
])

spark = (
    SparkSession.builder
    .appName("KafkaRawToMinio")
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# Чтение из Kafka
df = (
    spark.readStream
    .format("kafka")
    .option(key="kafka.bootstrap.servers", value=KAFKA_BOOTSTRAP_SERVERS)
    .option(key="subscribe", value=KAFKA_TOPIC)
    .option(key="startingOffsets", value="earliest")
    .load()
)

# value приводим к string и парсим json только верхнего уровня
json_df = df.selectExpr("CAST(value AS STRING) AS json_str")
json_df = json_df.withColumn(
    colName="event_params",
    col=from_json(col("json_str"), message_schema).getField("event_params"),
)
json_df = json_df.withColumn(
    colName="event_timestamp_ms",
    col=from_json(col("json_str"), message_schema).getField("event_timestamp_ms"),
)

# Сохраняем как есть (RAW), без нормализации
query = (
    json_df
    .writeStream
    .format("parquet")
    .option(key="path", value=f"s3a://{bucket_name}/")
    .option(key="checkpointLocation", value=f"/tmp/spark_checkpoint/{bucket_name}")
    .outputMode("append")
    .start()
)

print("Streaming started... Для остановки Ctrl+C")
query.awaitTermination()
