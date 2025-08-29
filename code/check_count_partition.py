import duckdb
import pandas as pd
from cred import access_key, secret_key

pd.set_option("display.max_columns", None)
conn = duckdb.connect(database=":memory:")

count_of_rows = conn.sql(
    f"""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_url_style = 'path';
    SET s3_endpoint = 'localhost:9000';
    SET s3_access_key_id = '{access_key}';
    SET s3_secret_access_key = '{secret_key}';
    SET s3_use_ssl = FALSE;

    SELECT
        COUNT(*) AS count_of_rows
    FROM
        's3://prod/*/*.parquet';
    """,
)

print(count_of_rows)

df_schema_of_model = conn.sql(
    f"""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_url_style = 'path';
    SET s3_endpoint = 'localhost:9000';
    SET s3_access_key_id = '{access_key}';
    SET s3_secret_access_key = '{secret_key}';
    SET s3_use_ssl = FALSE;

    SELECT path_in_schema, column_id FROM parquet_metadata('s3://prod/*/*.parquet');
    """,
)

print(df_schema_of_model)

first_10_rows = conn.sql(
    f"""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_url_style = 'path';
    SET s3_endpoint = 'localhost:9000';
    SET s3_access_key_id = '{access_key}';
    SET s3_secret_access_key = '{secret_key}';
    SET s3_use_ssl = FALSE;

    SELECT
        *
    FROM
        's3://prod/*/*.parquet'
    LIMIT 10;
    """,
)

print(first_10_rows)


first_10_rows_1_event_id = conn.sql(
    f"""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_url_style = 'path';
    SET s3_endpoint = 'localhost:9000';
    SET s3_access_key_id = '{access_key}';
    SET s3_secret_access_key = '{secret_key}';
    SET s3_use_ssl = FALSE;

    SELECT
        "event_params.event_type_id" AS event_type_id,
        "event_params.event_type" AS event_type,
        "event_params.user_id" AS user_id,
        "event_params.platform_token" AS platform_token,
        "event_params.ipv4" AS ipv4,
        "event_params.country" AS country,
        "event_params.uuid_track" AS uuid_track,
        "event_timestamp_ms.ts" AS ts,
        "event_timestamp_ms.ts_ms" AS ts_ms,
    FROM
        's3://prod/*/*.parquet'
    WHERE
        1=1
        AND "event_params.event_type_id" = 1
    LIMIT 10;
    """,
)

print(first_10_rows_1_event_id.show(max_col_width=9))


