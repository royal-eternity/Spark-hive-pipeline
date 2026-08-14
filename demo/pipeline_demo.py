"""
pipeline_demo.py
-----------------
Runnable local demonstration of the pipeline in src/pipeline.py.

This demo stands in for a real Hadoop/Hive cluster:
  - "Web API" source  -> data/webapi/customer_transactions.json   (simulates the API pull)
  - "HDFS" source      -> data/hdfs_raw/customer_engagement.json  (simulates data already in HDFS)
  - "Hive table"       -> written via saveAsTable() into Spark's managed catalog
                           (spark-warehouse/), the same call used against a real Hive metastore.

It imports and calls the exact same functions used by the production job
(flatten_webapi_source, flatten_hdfs_source, stitch, cleanse_and_transform) —
nothing here is a separate reimplementation.

Run with:  python3 demo/pipeline_demo.py
"""

import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from pipeline import (  # noqa: E402
    flatten_webapi_source,
    flatten_hdfs_source,
    stitch,
    cleanse_and_transform,
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEBAPI_SRC = os.path.join(BASE_DIR, "data", "webapi", "customer_transactions.json")
HDFS_SRC = os.path.join(BASE_DIR, "data", "hdfs_raw", "customer_engagement.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
WAREHOUSE_DIR = os.path.join(BASE_DIR, "spark-warehouse")

HIVE_DB = "analytics"
HIVE_TABLE = "customer_360_curated"


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
    stitched = stitch(webapi_flat, hdfs_flat)
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
