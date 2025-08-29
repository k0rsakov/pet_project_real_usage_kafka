from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

from cred import access_key, secret_key

minio_endpoint = "http://localhost:9000"
bucket_name = "prod-spark"

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
KAFKA_TOPIC = "music_events"

# Определяем только верхнеуровневую структуру
event_params_schema = StructType([])
event_timestamp_schema = StructType([])

message_schema = StructType([
    StructField("event_params", StringType()),  # Можно StringType, если не хотим парсить глубже
    StructField("event_timestamp_ms", StringType())
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
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# value приводим к string и парсим json только верхнего уровня
json_df = df.selectExpr("CAST(value AS STRING) as json_str")
json_df = json_df.withColumn("event_params", from_json(col("json_str"), message_schema).getField("event_params"))
json_df = json_df.withColumn("event_timestamp_ms", from_json(col("json_str"), message_schema).getField("event_timestamp_ms"))

# Сохраняем как есть (RAW), без нормализации
query = (
    json_df
    .writeStream
    .format("parquet")
    .option("path", f"s3a://{bucket_name}/")
    .option("checkpointLocation", f"/tmp/spark_checkpoint/{bucket_name}")
    .outputMode("append")
    .start()
)

print("Streaming started... Для остановки Ctrl+C")
query.awaitTermination()