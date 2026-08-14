"""
pipeline_demo.py
-----------------
Runnable local demonstration of the Spark-Hive Complex JSON Processing
pipeline described in src/main/scala/com/project/sparkhive/SparkHiveJsonPipeline.scala

This demo stands in for the real cluster:
  - "Web API" source  -> data/webapi/customer_transactions.json   (simulates the API pull)
  - "HDFS" source      -> data/hdfs_raw/customer_engagement.json  (simulates data already in HDFS)
  - "Hive table"       -> written as a partitioned Parquet table registered in Spark's
                           managed catalog (spark-warehouse/), using saveAsTable(),
                           the same call used against a real Hive metastore.

Run with:  python3 demo/pipeline_demo.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEBAPI_SRC = os.path.join(BASE_DIR, "data", "webapi", "customer_transactions.json")
HDFS_SRC = os.path.join(BASE_DIR, "data", "hdfs_raw", "customer_engagement.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
WAREHOUSE_DIR = os.path.join(BASE_DIR, "spark-warehouse")

HIVE_DB = "analytics"
HIVE_TABLE = "customer_360_curated"


def flatten_webapi_source(df):
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


def flatten_hdfs_source(df):
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


def cleanse_and_transform(df):
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


def main():
    spark = (
        SparkSession.builder.appName("Spark-Hive-Complex-JSON-Processing-DEMO")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("spark.sql.shuffle.partitions", "4")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n================ STAGE 1: READ FROM WEB API SOURCE ================\n")
    webapi_df = spark.read.json(WEBAPI_SRC)
    webapi_df.printSchema()
    webapi_df.show(5, truncate=60)

    print("\n================ STAGE 2: READ FROM HDFS SOURCE ================\n")
    hdfs_df = spark.read.json(HDFS_SRC)
    hdfs_df.printSchema()
    hdfs_df.show(5, truncate=60)

    print("\n================ STAGE 3: FLATTEN / STAGE BOTH SOURCES ================\n")
    webapi_flat = flatten_webapi_source(webapi_df)
    hdfs_flat = flatten_hdfs_source(hdfs_df)
    print(">>> WebAPI staged DataFrame:")
    webapi_flat.show(10, truncate=40)
    print(">>> HDFS staged DataFrame:")
    hdfs_flat.show(10, truncate=40)

    webapi_flat.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "stage_webapi"))
    hdfs_flat.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "stage_hdfs"))

    print("\n================ STAGE 4: STITCH (JOIN) HDFS -> WEB API ================\n")
    stitched = webapi_flat.join(hdfs_flat, on="customer_id", how="left_outer")
    stitched.show(10, truncate=40)

    print("\n================ STAGE 5: CLEANSE + TRANSFORM ================\n")
    curated = cleanse_and_transform(stitched)
    curated.orderBy("customer_id").show(10, truncate=40)

    curated.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "curated"))

    print("\n================ STAGE 6: WRITE TO HIVE TABLE ================\n")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
    curated.write.mode("overwrite").format("hive").partitionBy("region").saveAsTable(
        f"{HIVE_DB}.{HIVE_TABLE}"
    )

    print(f"\n>>> Tables now in Hive-compatible catalog under '{HIVE_DB}':")
    spark.sql(f"SHOW TABLES IN {HIVE_DB}").show(truncate=False)

    print(f"\n>>> SELECT * FROM {HIVE_DB}.{HIVE_TABLE}:")
    spark.sql(f"SELECT * FROM {HIVE_DB}.{HIVE_TABLE} ORDER BY customer_id").show(20, truncate=40)

    print("\n================ ANALYTICS PREVIEW: high-value + engaged customers ================\n")
    spark.sql(f"""
        SELECT customer_id, name, region, loyalty_tier, txn_amount, engagement_score, is_high_value
        FROM {HIVE_DB}.{HIVE_TABLE}
        WHERE is_high_value = true OR engagement_score > 20
        ORDER BY engagement_score DESC
    """).show(20, truncate=40)

    row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {HIVE_DB}.{HIVE_TABLE}").collect()[0]["cnt"]
    print(f"\n>>> Final curated row count written to Hive: {row_count}")

    spark.stop()


if __name__ == "__main__":
    main()
