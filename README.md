# Spark-Hive: Complex JSON Data Processing

**Multi-source Spark ingestion → HDFS staging → data stitching → Hive analytics table**

![Spark](https://img.shields.io/badge/Apache%20Spark-3.4-E25A1C?logo=apachespark&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop-3.0-66CCFF?logo=apachehadoop&logoColor=white)
![Hive](https://img.shields.io/badge/Apache%20Hive-FFCC00?logo=apachehive&logoColor=black)
![Scala](https://img.shields.io/badge/Scala-2.12-DC322F?logo=scala&logoColor=white)
![Status](https://img.shields.io/badge/status-demo%20verified-brightgreen)

---

## 1. Overview

This project ingests **complex, deeply nested JSON** from two independent sources — a **Web API** and **HDFS** — loads it into Spark DataFrames, stages it through multiple transformation zones, **stitches** the two datasets together on `customer_id`, and pushes the curated, analytics-ready result into a **partitioned Hive table** for downstream predictive analytics.

| | |
|---|---|
| **Sources** | REST Web API (customer + transactions) · HDFS raw zone (engagement + loyalty logs) |
| **Processing engine** | Apache Spark (Scala, DataFrame API) |
| **Storage layers** | HDFS landing → HDFS staging (Parquet) → Hive curated table (Parquet, partitioned) |
| **Output** | `analytics.customer_360_curated` Hive table, partitioned by `region` |
| **Core techniques** | Nested JSON schema inference, `explode`/struct flattening, multi-source join ("stitching"), null-safe cleansing, derived scoring metrics |

---

## 2. Architecture

```mermaid
flowchart LR
    subgraph SRC["Data Sources"]
        A1["Web API
(complex nested JSON
customers + transactions)"]
        A2["HDFS Raw Zone
(complex nested JSON
engagement + loyalty)"]
    end

    subgraph INGEST["Ingestion"]
        B1["Land raw JSON
into HDFS landing dir"]
        B2["Read JSON already
resident in HDFS"]
    end

    subgraph SPARK["Spark Processing (Scala)"]
        C1["DataFrame:
webapi_raw"]
        C2["DataFrame:
hdfs_raw"]
        D1["Flatten / Explode
structs + arrays"]
        D2["Flatten / Explode
structs + arrays"]
        E["Staging Zone
(Parquet, HDFS)"]
        F["Stitch
left-outer JOIN
on customer_id"]
        G["Cleanse & Transform
nulls, dedup, scoring"]
    end

    subgraph HIVE["Hive Warehouse"]
        H["customer_360_curated
(partitioned by region)"]
    end

    subgraph BI["Consumption"]
        I["Predictive Analytics /
BI Dashboards"]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C2
    C1 --> D1 --> E
    C2 --> D2 --> E
    E --> F --> G --> H --> I
```

### Pipeline stage flow

```mermaid
flowchart TD
    S0["Stage 0
Create HDFS directories"] --> S1
    S1["Stage 1
Fetch complex JSON from Web API
and land it in HDFS"] --> S2
    S2["Stage 2
Read complex JSON already
resident in HDFS"] --> S3
    S3["Stage 3
Flatten nested structs/arrays
into staged DataFrames"] --> S4
    S4["Stage 4
Stitch: left-outer join
HDFS data onto Web API data
on customer_id"] --> S5
    S5["Stage 5
Cleanse + Transform
(null handling, dedup,
engagement scoring, flags)"] --> S6
    S6["Stage 6
Write curated DataFrame
to partitioned Hive table"] --> S7
    S7["Stage 7
Serve Hive table to
predictive analytics / BI"]
```

---

## 3. Repository structure

```
spark-hive-json-processing/
├── README.md
├── build.sbt                                # Scala/SBT build definition
├── src/main/scala/com/project/sparkhive/
│   ├── SparkHiveJsonPipeline.scala          # Main Spark job (production Scala code)
│   └── hive_ddl.sql                         # Hive table DDL (landing/raw/curated)
├── demo/
│   ├── pipeline_demo.py                     # Runnable PySpark demo (mirrors the Scala logic)
│   ├── make_screenshots.py                  # Generates the screenshots in /screenshots
│   └── requirements.txt
├── data/
│   ├── webapi/customer_transactions.json    # Sample complex nested JSON (Web API source)
│   └── hdfs_raw/customer_engagement.json    # Sample complex nested JSON (HDFS source)
├── diagrams/
│   ├── architecture.mmd
│   └── pipeline_stages.mmd
├── output/
│   └── run_log_clean.txt                    # Full captured console output of a real run
└── screenshots/                             # PNG screenshots of the actual run output
```

---

## 4. Sample input data (complex nested JSON)

**Web API source** — `data/webapi/customer_transactions.json`
```json
{
  "customer_id": "CUST1001",
  "name": "Ananya Rao",
  "region": "APAC",
  "contact": {
    "email": "ananya.rao@example.com",
    "address": { "city": "Bengaluru", "country": "India", "zip": "560001" }
  },
  "transactions": [
    { "txn_id": "TXN9001", "amount": 4599.50, "category": "electronics", "status": "completed" },
    { "txn_id": "TXN9002", "amount": 1200.00, "category": "groceries",   "status": "completed" }
  ],
  "preferences": { "newsletter": true, "channels": ["email", "sms"] }
}
```

**HDFS source** — `data/hdfs_raw/customer_engagement.json`
```json
{
  "customer_id": "CUST1001",
  "sessions": [
    { "session_id": "SESS501", "device": { "type": "mobile", "os": "Android" },
      "duration_sec": 320, "pages_viewed": ["home", "electronics", "product/4599"] }
  ],
  "loyalty": { "tier": "gold", "points": 4200 }
}
```

Both files contain nested **structs**, **arrays of structs**, and **arrays of primitives** — the kind of shape a flat `CREATE TABLE` can't represent, which is why the pipeline explicitly flattens each source before joining.

---

## 5. Core Scala/Spark logic

**Flattening a nested source** (`SparkHiveJsonPipeline.scala`):
```scala
def flattenWebApiSource(df: DataFrame): DataFrame = {
  df.withColumn("txn", explode_outer($"transactions"))
    .select(
      $"customer_id", $"name", $"region", $"signup_date",
      $"contact.email".as("email"),
      $"contact.address.city".as("city"),
      $"txn.txn_id".as("txn_id"),
      $"txn.amount".as("txn_amount"),
      $"txn.status".as("txn_status"),
      $"preferences.newsletter".as("newsletter_opt_in")
    )
}
```

**Stitching the two sources together:**
```scala
val stitchedDF: DataFrame = webApiFlatDF
  .join(hdfsFlatDF, Seq("customer_id"), "left_outer")
```

**Cleansing, transformation, and derived metrics:**
```scala
def cleanseAndTransform(df: DataFrame): DataFrame = {
  df.na.fill(Map("loyalty_tier" -> "unrated", "txn_status" -> "unknown"))
    .withColumn("txn_amount", coalesce($"txn_amount", lit(0.0)))
    .dropDuplicates("customer_id", "txn_id")
    .withColumn("engagement_score",
      round(($"total_engagement_sec" / 60.0) * (col("avg_pages_per_session") + lit(1)), 2))
    .withColumn("is_high_value", when($"txn_amount" > 5000, true).otherwise(false))
}
```

**Writing to Hive (partitioned):**
```scala
curatedDF.write
  .mode("overwrite")
  .format("hive")
  .partitionBy("region")
  .saveAsTable("analytics.customer_360_curated")
```

Full source: [`src/main/scala/com/project/sparkhive/SparkHiveJsonPipeline.scala`](src/main/scala/com/project/sparkhive/SparkHiveJsonPipeline.scala)
Hive DDL: [`src/main/scala/com/project/sparkhive/hive_ddl.sql`](src/main/scala/com/project/sparkhive/hive_ddl.sql)

---

## 6. Curated Hive table schema

| Column | Type | Source | Notes |
|---|---|---|---|
| `customer_id` | STRING | both | join key |
| `name`, `signup_date`, `email`, `city`, `country` | STRING | Web API | flattened from nested `contact` struct |
| `txn_id`, `txn_amount`, `txn_currency`, `txn_category`, `txn_status`, `txn_timestamp` | STRING/DOUBLE | Web API | one row per exploded transaction |
| `newsletter_opt_in` | BOOLEAN | Web API | flattened from `preferences` struct |
| `loyalty_tier`, `loyalty_points` | STRING/INT | HDFS | flattened from `loyalty` struct |
| `total_sessions`, `total_engagement_sec`, `avg_pages_per_session` | BIGINT/DOUBLE | HDFS | aggregated from exploded `sessions` array |
| `engagement_score` | DOUBLE | derived | `(total_engagement_sec / 60) × (avg_pages_per_session + 1)` |
| `is_high_value` | BOOLEAN | derived | `true` when `txn_amount > 5000` |
| `load_timestamp` | TIMESTAMP | derived | pipeline run time |
| `region` | STRING | Web API | **partition column** |

---

## 7. Running it yourself

The Scala job targets a real Hadoop/HDFS/Hive cluster (`spark-submit` against YARN, with `hive-site.xml` on the classpath). To let anyone verify the *logic* without standing up a cluster, this repo also ships a **runnable PySpark demo that mirrors the exact same stages** and writes to a local Hive-compatible catalog.

### Option A — Real cluster (Scala)
```bash
sbt clean assembly
spark-submit \
  --class com.project.sparkhive.SparkHiveJsonPipeline \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.12/spark-hive-json-processing-assembly-1.0.0.jar
```

### Option B — Local demo (PySpark, no cluster needed)
```bash
pip install -r demo/requirements.txt
python3 demo/pipeline_demo.py
```

---

## 8. Verified run output

The commands above were actually executed for this repo. Full raw console output: [`output/run_log_clean.txt`](output/run_log_clean.txt).

**Stage 1 — reading the nested Web API JSON into a DataFrame:**

![Stage 1 output](screenshots/01_stage1_webapi_ingest.png)

**Stage 4 — stitching the HDFS engagement data onto the Web API data:**

![Stage 4 output](screenshots/02_stage4_stitch_join.png)

**Final curated table, as it lands in Hive:**

![Hive table output](screenshots/03_hive_table_output.png)

**Downstream analytics query against the curated Hive table:**

![Analytics query output](screenshots/04_analytics_query_output.png)

---

## 9. Why this design

| Decision | Reasoning |
|---|---|
| Flatten *before* joining | Nested arrays can't be joined directly; each source is exploded into a row-per-record shape first |
| `left_outer` join, WebAPI → HDFS | Every paying customer should appear even with no engagement data; the reverse isn't true |
| Staging written as Parquet | Columnar, compressed, schema-preserving — cheaper for Spark to re-read than re-parsing JSON |
| Partition Hive table by `region` | Predictive analytics queries in this domain are almost always region-scoped; partition pruning cuts scan cost |
| Null-safe cleansing before scoring | Prevents `NULL` propagation from silently dropping rows out of downstream aggregates |

---

## 10. Tech stack

`Hadoop 3.0` · `HDFS` · `Apache Hive` · `Apache Spark 3.4 (Scala 2.12)` · `Spark SQL / DataFrame API` · `Parquet` · `SBT`

---

## 11. Resume bullet points (reference)

- Built a Spark pipeline in Scala to ingest complex, deeply nested JSON from a Web API and HDFS, flattening and staging both sources through multiple transformation zones.
- Designed and implemented HDFS directory structure and Hive tables to support raw, staging, and curated data zones.
- Stitched (joined) HDFS engagement/loyalty data onto Web API transaction data on `customer_id` to build a unified customer view for predictive analytics.
- Implemented cleansing and transformation logic (null handling, deduplication, derived scoring metrics) and loaded curated output into a partitioned Hive table.

---

## License

MIT — see [LICENSE](LICENSE).
