-- ============================================================
-- Hive DDL: Spark-Hive Complex JSON Processing
-- ============================================================

CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;

-- Raw landing table over the Web API JSON dropped into HDFS
CREATE EXTERNAL TABLE IF NOT EXISTS raw_webapi_transactions (
    customer_id   STRING,
    name          STRING,
    region        STRING,
    signup_date   STRING,
    contact       STRUCT<email:STRING, phone:STRING,
                          address:STRUCT<city:STRING, state:STRING, country:STRING, zip:STRING>>,
    transactions  ARRAY<STRUCT<txn_id:STRING, amount:DOUBLE, currency:STRING,
                                category:STRING, timestamp:STRING, status:STRING>>,
    preferences   STRUCT<newsletter:BOOLEAN, channels:ARRAY<STRING>>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/data/landing/webapi/customer_transactions';

-- Raw HDFS-native engagement/loyalty source
CREATE EXTERNAL TABLE IF NOT EXISTS raw_hdfs_engagement (
    customer_id STRING,
    sessions    ARRAY<STRUCT<session_id:STRING,
                              device:STRUCT<type:STRING, os:STRING, app_version:STRING>,
                              duration_sec:INT,
                              pages_viewed:ARRAY<STRING>,
                              date:STRING>>,
    loyalty     STRUCT<tier:STRING, points:INT, last_updated:STRING>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/data/raw/hdfs_source/customer_engagement';

-- Final curated, stitched table written by the Spark job (Parquet + partitioned)
CREATE TABLE IF NOT EXISTS customer_360_curated (
    customer_id           STRING,
    name                  STRING,
    signup_date           STRING,
    email                 STRING,
    city                  STRING,
    country               STRING,
    txn_id                STRING,
    txn_amount            DOUBLE,
    txn_currency          STRING,
    txn_category          STRING,
    txn_status            STRING,
    txn_timestamp         STRING,
    newsletter_opt_in     BOOLEAN,
    loyalty_tier          STRING,
    loyalty_points        INT,
    total_sessions        BIGINT,
    total_engagement_sec  BIGINT,
    avg_pages_per_session DOUBLE,
    engagement_score      DOUBLE,
    is_high_value         BOOLEAN,
    load_timestamp        TIMESTAMP
)
PARTITIONED BY (region STRING)
STORED AS PARQUET;
