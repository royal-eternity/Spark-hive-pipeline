"""
pipeline.py
-----------
Spark-Hive Complex JSON Data Processing

Reads complex, deeply nested JSON from two sources — a Web API and HDFS —
loads them into Spark DataFrames, stages both through a flatten step,
stitches (joins) them together on customer_id, applies cleansing and
transformation, and writes the curated result into a partitioned Hive table
for downstream predictive analytics.

Usage (against a real Hadoop/Hive cluster):
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        src/pipeline.py \
        --webapi-url https://internal-api.example.com/v1/customers/transactions \
        --hdfs-source-dir /data/raw/hdfs_source/customer_engagement \
        --hdfs-landing-dir /data/landing/webapi/customer_transactions \
        --hdfs-staging-dir /data/staging \
        --hive-db analytics \
        --hive-table customer_360_curated

Usage (local demo, no cluster required — see demo/pipeline_demo.py):
    python3 demo/pipeline_demo.py
"""

import argparse
import urllib.request

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def fetch_from_web_api(url: str) -> str:
    """Pull the raw complex JSON payload from the Web API source."""
    with urllib.request.urlopen(url, timeout=10) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Web API call failed with status {resp.status}")
        return resp.read().decode("utf-8")


def write_raw_json_to_hdfs(spark: SparkSession, payload: str, path: str) -> None:
    """Land the raw JSON payload into HDFS before Spark parses it (raw zone)."""
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    out_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    out_stream = fs.create(out_path, True)
    out_stream.writeBytes(payload)
    out_stream.close()


def ensure_hdfs_dirs(spark: SparkSession, dirs) -> None:
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    for d in dirs:
        p = sc._jvm.org.apache.hadoop.fs.Path(d)
        if not fs.exists(p):
            fs.mkdirs(p)


def flatten_webapi_source(df: DataFrame) -> DataFrame:
    """Explode + flatten the WebAPI nested JSON (contact struct, transactions array)."""
    exploded = df.withColumn("txn", F.explode_outer("transactions"))
    return exploded.select(
        F.col("customer_id"),
        F.col("name"),
        F.col("region"),
        F.col("signup_date"),
        F.col("contact.email").alias("email"),
        F.col("contact.address.city").alias("city"),
        F.col("contact.address.country").alias("country"),
        F.col("txn.txn_id").alias("txn_id"),
        F.col("txn.amount").alias("txn_amount"),
        F.col("txn.currency").alias("txn_currency"),
        F.col("txn.category").alias("txn_category"),
        F.col("txn.status").alias("txn_status"),
        F.col("txn.timestamp").alias("txn_timestamp"),
        F.col("preferences.newsletter").alias("newsletter_opt_in"),
    )


def flatten_hdfs_source(df: DataFrame) -> DataFrame:
    """Explode + flatten the HDFS nested JSON (sessions array, device/loyalty structs)."""
    exploded = df.withColumn("session", F.explode_outer("sessions"))
    sessions_flat = exploded.select(
        F.col("customer_id"),
        F.col("session.session_id").alias("session_id"),
        F.col("session.device.type").alias("device_type"),
        F.col("session.device.os").alias("device_os"),
        F.col("session.duration_sec").alias("session_duration_sec"),
        F.size("session.pages_viewed").alias("pages_viewed_count"),
        F.col("session.date").alias("session_date"),
        F.col("loyalty.tier").alias("loyalty_tier"),
        F.col("loyalty.points").alias("loyalty_points"),
    )
    return sessions_flat.groupBy("customer_id", "loyalty_tier", "loyalty_points").agg(
        F.count("session_id").alias("total_sessions"),
        F.sum("session_duration_sec").alias("total_engagement_sec"),
        F.avg("pages_viewed_count").alias("avg_pages_per_session"),
    )


def stitch(webapi_flat: DataFrame, hdfs_flat: DataFrame) -> DataFrame:
    """Stitch HDFS engagement/loyalty data onto Web API transaction data."""
    return webapi_flat.join(hdfs_flat, on="customer_id", how="left_outer")


def cleanse_and_transform(df: DataFrame) -> DataFrame:
    """Null handling, dedup, and derived scoring metrics."""
    df = df.fillna({
        "loyalty_tier": "unrated",
        "total_sessions": 0,
        "total_engagement_sec": 0,
        "txn_status": "unknown",
    })
    df = df.withColumn("txn_amount", F.coalesce(F.col("txn_amount"), F.lit(0.0)))
    df = df.filter(F.col("customer_id").isNotNull())
    df = df.dropDuplicates(["customer_id", "txn_id"])
    df = df.withColumn(
        "engagement_score",
        F.round((F.col("total_engagement_sec") / 60.0) * (F.col("avg_pages_per_session") + F.lit(1)), 2),
    )
    df = df.withColumn("is_high_value", F.when(F.col("txn_amount") > 5000, True).otherwise(False))
    df = df.withColumn("load_timestamp", F.current_timestamp())
    return df


def run_pipeline(
    spark: SparkSession,
    webapi_json_path_or_url: str,
    hdfs_source_dir: str,
    hdfs_landing_dir: str,
    hdfs_staging_dir: str,
    hive_db: str,
    hive_table: str,
    fetch_over_http: bool = False,
) -> DataFrame:
    """Runs the full pipeline end to end and returns the curated DataFrame."""

    # Stage 1: ingest from Web API (or read a local/HDFS JSON file standing in for it)
    if fetch_over_http:
        payload = fetch_from_web_api(webapi_json_path_or_url)
        write_raw_json_to_hdfs(spark, payload, f"{hdfs_landing_dir}/customer_transactions.json")
        webapi_df = spark.read.json(f"{hdfs_landing_dir}/customer_transactions.json")
    else:
        webapi_df = spark.read.json(webapi_json_path_or_url)

    # Stage 2: read the second complex JSON source already sitting in HDFS
    hdfs_df = spark.read.json(hdfs_source_dir)

    # Stage 3: flatten nested structs/arrays for each source, persist to staging
    webapi_flat = flatten_webapi_source(webapi_df)
    hdfs_flat = flatten_hdfs_source(hdfs_df)
    webapi_flat.write.mode("overwrite").parquet(f"{hdfs_staging_dir}/webapi_stage")
    hdfs_flat.write.mode("overwrite").parquet(f"{hdfs_staging_dir}/hdfs_stage")

    # Stage 4: stitch
    stitched = stitch(webapi_flat, hdfs_flat)

    # Stage 5: cleanse + transform
    curated = cleanse_and_transform(stitched)
    curated.write.mode("overwrite").parquet(f"{hdfs_staging_dir}/curated")

    # Stage 6: push to Hive
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
    curated.write.mode("overwrite").format("hive").partitionBy("region").saveAsTable(
        f"{hive_db}.{hive_table}"
    )

    return curated


def main():
    parser = argparse.ArgumentParser(description="Spark-Hive Complex JSON Data Processing")
    parser.add_argument("--webapi-url", required=True, help="Web API endpoint or local/HDFS JSON path")
    parser.add_argument("--hdfs-source-dir", required=True)
    parser.add_argument("--hdfs-landing-dir", required=True)
    parser.add_argument("--hdfs-staging-dir", required=True)
    parser.add_argument("--hive-db", default="analytics")
    parser.add_argument("--hive-table", default="customer_360_curated")
    parser.add_argument("--fetch-over-http", action="store_true")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("Spark-Hive-Complex-JSON-Processing")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    curated = run_pipeline(
        spark,
        args.webapi_url,
        args.hdfs_source_dir,
        args.hdfs_landing_dir,
        args.hdfs_staging_dir,
        args.hive_db,
        args.hive_table,
        fetch_over_http=args.fetch_over_http,
    )
    curated.show(20, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
