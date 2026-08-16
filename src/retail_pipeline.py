from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("UCI Online Retail Pipeline")
    .config("spark.sql.shuffle.partitions", "8")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n========== SPARK STARTED ==========\n")


# ==========================================================
# READ 1 MILLION+ ROW DATASET
# ==========================================================

input_file = r"C:\Big data Engineering\spark-hive-json\data\Raw\online_retail_II.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_file)
)

print("========== RAW DATA ==========")

raw_count = df.count()

print("TOTAL ROWS READ:", raw_count)

df.printSchema()

df.show(10, truncate=False)


# ==========================================================
# RENAME COLUMNS
# ==========================================================

df = (
    df
    .withColumnRenamed("Invoice", "invoice_id")
    .withColumnRenamed("StockCode", "product_id")
    .withColumnRenamed("Description", "product_name")
    .withColumnRenamed("Quantity", "quantity")
    .withColumnRenamed("InvoiceDate", "invoice_date")
    .withColumnRenamed("Price", "unit_price")
    .withColumnRenamed("Customer ID", "customer_id")
    .withColumnRenamed("Country", "country")
)


# ==========================================================
# CLEAN DATA
# ==========================================================

df = df.dropDuplicates()

df = df.filter(
    F.col("quantity").isNotNull()
)

df = df.filter(
    F.col("unit_price").isNotNull()
)

df = df.filter(
    F.col("quantity") != 0
)

df = df.filter(
    F.col("unit_price") >= 0
)


# ==========================================================
# CUSTOMER ID
# ==========================================================

df = df.withColumn(
    "customer_id",
    F.col("customer_id").cast("string")
)

df = df.withColumn(
    "customer_id",
    F.when(
        F.col("customer_id").isNull(),
        "UNKNOWN"
    ).otherwise(F.col("customer_id"))
)


# ==========================================================
# CALCULATE TOTAL AMOUNT
# ==========================================================

df = df.withColumn(
    "total_amount",
    F.round(
        F.col("quantity") * F.col("unit_price"),
        2
    )
)


# ==========================================================
# CONVERT DATE
# ==========================================================

df = df.withColumn(
    "invoice_date",
    F.to_timestamp("invoice_date")
)


# ==========================================================
# CLEANED DATA RESULT
# ==========================================================

print("\n========== CLEANED DATA ==========")

clean_count = df.count()

print("ROWS AFTER CLEANING:", clean_count)

df.show(10, truncate=False)

# ==========================================================
# EXPORT ALL CLEANED ROWS TO CSV
# ==========================================================

output_csv = r"C:\Big data Engineering\spark-hive-json\output\retail_cleaned_csv"

print("\nWriting ALL cleaned rows to CSV...")

df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_csv)

print("\n========== ALL ROWS EXPORTED TO CSV ==========")
print("Rows exported:", clean_count)
print("Location:", output_csv)
# ==========================================================
# WRITE PARQUET STAGING
# ==========================================================

staging_path = r"C:\Big data Engineering\spark-hive-json\output\retail_staging"

df.write \
    .mode("overwrite") \
    .parquet(staging_path)

print("\nPARQUET STAGING CREATED")


# ==========================================================
# CREATE HIVE DATABASE
# ==========================================================

spark.sql("""
CREATE DATABASE IF NOT EXISTS retail
""")

print("\nHIVE DATABASE 'retail' READY")



# ==========================================================
# WRITE HIVE TABLE
# ==========================================================

# Allow Hive to create partitions dynamically
spark.sql("SET hive.exec.dynamic.partition=true")
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

df.write \
    .mode("overwrite") \
    .format("hive") \
    .partitionBy("country") \
    .saveAsTable("retail.transactions")



print("\n========== HIVE TABLE CREATED ==========")

# ==========================================================
# VERIFY HIVE TABLE
# ==========================================================

print("\n========== HIVE ROW COUNT ==========")

spark.sql("""
SELECT COUNT(*) AS total_records
FROM retail.transactions
""").show()


# ==========================================================
# TOP COUNTRIES
# ==========================================================

print("\n========== TOP COUNTRIES ==========")

spark.sql("""
SELECT
    country,
    COUNT(*) AS transaction_count
FROM retail.transactions
GROUP BY country
ORDER BY transaction_count DESC
LIMIT 10
""").show()


# ==========================================================
# TOP CUSTOMERS
# ==========================================================

print("\n========== TOP CUSTOMERS ==========")

spark.sql("""
SELECT
    customer_id,
    ROUND(SUM(total_amount), 2) AS total_spending
FROM retail.transactions
WHERE customer_id != 'UNKNOWN'
GROUP BY customer_id
ORDER BY total_spending DESC
LIMIT 10
""").show()


# ==========================================================
# FINISH
# ==========================================================

spark.stop()

print("\n========== PIPELINE COMPLETED SUCCESSFULLY ==========\n")