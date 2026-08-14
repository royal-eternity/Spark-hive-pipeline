package com.project.sparkhive

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scalaj.http.Http
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * SparkHiveJsonPipeline
 *
 * End-to-end pipeline that:
 *   1. Pulls complex/nested JSON from a Web API source (customer + transactions).
 *   2. Reads complex/nested JSON already landed in HDFS (engagement + loyalty logs).
 *   3. Flattens both into staged DataFrames.
 *   4. Stitches (joins) the two sources on customer_id for a unified 360-degree view.
 *   5. Applies cleansing + transformation rules.
 *   6. Writes the final curated dataset into a Hive table for downstream
 *      predictive-analytics consumption.
 */
object SparkHiveJsonPipeline {

  val HDFS_LANDING_DIR   = "/data/landing/webapi/customer_transactions"
  val HDFS_RAW_DIR       = "/data/raw/hdfs_source/customer_engagement"
  val HDFS_STAGE_DIR     = "/data/staging/"
  val HIVE_DB            = "analytics"
  val HIVE_TABLE         = "customer_360_curated"
  val WEB_API_URL        = "https://internal-api.example.com/v1/customers/transactions"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark-Hive-Complex-JSON-Processing")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // ---------- STAGE 0: Ensure HDFS directories exist ----------
    createHdfsDirsIfMissing(spark, Seq(HDFS_LANDING_DIR, HDFS_RAW_DIR, HDFS_STAGE_DIR))

    // ---------- STAGE 1: Ingest from Web API, land raw JSON into HDFS ----------
    val webApiRawJson: String = fetchFromWebApi(WEB_API_URL)
    writeRawJsonToHdfs(spark, webApiRawJson, s"$HDFS_LANDING_DIR/customer_transactions.json")

    val webApiDF: DataFrame = spark.read
      .option("multiLine", "false")
      .json(s"$HDFS_LANDING_DIR/customer_transactions.json")

    // ---------- STAGE 2: Read the second complex JSON source already sitting in HDFS ----------
    val hdfsSourceDF: DataFrame = spark.read
      .option("multiLine", "false")
      .json(HDFS_RAW_DIR)

    // ---------- STAGE 3: Flatten nested structs / arrays for each source ----------
    val webApiFlatDF = flattenWebApiSource(webApiDF)
    val hdfsFlatDF    = flattenHdfsSource(hdfsSourceDF)

    // Persist intermediate staged DataFrames back to HDFS (staging area)
    webApiFlatDF.write.mode("overwrite").parquet(s"$HDFS_STAGE_DIR/webapi_stage")
    hdfsFlatDF.write.mode("overwrite").parquet(s"$HDFS_STAGE_DIR/hdfs_stage")

    // ---------- STAGE 4: Stitch (join) HDFS engagement data onto WebAPI transaction data ----------
    val stitchedDF: DataFrame = webApiFlatDF
      .join(hdfsFlatDF, Seq("customer_id"), "left_outer")

    // ---------- STAGE 5: Cleanse + transform ----------
    val curatedDF: DataFrame = cleanseAndTransform(stitchedDF)

    curatedDF.write.mode("overwrite").parquet(s"$HDFS_STAGE_DIR/curated")

    // ---------- STAGE 6: Push crunched data into Hive ----------
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $HIVE_DB")
    curatedDF.write
      .mode("overwrite")
      .format("hive")
      .partitionBy("region")
      .saveAsTable(s"$HIVE_DB.$HIVE_TABLE")

    spark.sql(s"SELECT * FROM $HIVE_DB.$HIVE_TABLE").show(20, truncate = false)

    spark.stop()
  }

  /** Fetch complex nested JSON payload from an external Web API. */
  def fetchFromWebApi(url: String): String = {
    val response = Http(url)
      .header("Accept", "application/json")
      .timeout(connTimeoutMs = 5000, readTimeoutMs = 10000)
      .asString
    require(response.code == 200, s"Web API call failed with status ${response.code}")
    response.body
  }

  /** Land raw JSON payload into HDFS before it is ever parsed by Spark (raw zone). */
  def writeRawJsonToHdfs(spark: SparkSession, payload: String, path: String): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    val out = fs.create(new Path(path), true)
    out.writeBytes(payload)
    out.close()
  }

  def createHdfsDirsIfMissing(spark: SparkSession, dirs: Seq[String]): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    dirs.foreach { d =>
      val p = new Path(d)
      if (!fs.exists(p)) fs.mkdirs(p)
    }
  }

  /** Explode + flatten the WebAPI nested JSON (contact struct, transactions array). */
  def flattenWebApiSource(df: DataFrame): DataFrame = {
    df.withColumn("txn", explode_outer($"transactions"))
      .select(
        $"customer_id",
        $"name",
        $"region",
        $"signup_date",
        $"contact.email".as("email"),
        $"contact.address.city".as("city"),
        $"contact.address.country".as("country"),
        $"txn.txn_id".as("txn_id"),
        $"txn.amount".as("txn_amount"),
        $"txn.currency".as("txn_currency"),
        $"txn.category".as("txn_category"),
        $"txn.status".as("txn_status"),
        $"txn.timestamp".as("txn_timestamp"),
        $"preferences.newsletter".as("newsletter_opt_in")
      )
  }

  /** Explode + flatten the HDFS nested JSON (sessions array, device struct, loyalty struct). */
  def flattenHdfsSource(df: DataFrame): DataFrame = {
    df.withColumn("session", explode_outer($"sessions"))
      .select(
        $"customer_id",
        $"session.session_id".as("session_id"),
        $"session.device.type".as("device_type"),
        $"session.device.os".as("device_os"),
        $"session.duration_sec".as("session_duration_sec"),
        size($"session.pages_viewed").as("pages_viewed_count"),
        $"session.date".as("session_date"),
        $"loyalty.tier".as("loyalty_tier"),
        $"loyalty.points".as("loyalty_points")
      )
      .groupBy($"customer_id", $"loyalty_tier", $"loyalty_points")
      .agg(
        count($"session_id").as("total_sessions"),
        sum($"session_duration_sec").as("total_engagement_sec"),
        avg($"pages_viewed_count").as("avg_pages_per_session")
      )
  }

  /** Cleansing + business transformations applied to the stitched DataFrame. */
  def cleanseAndTransform(df: DataFrame): DataFrame = {
    df
      .na.fill(Map(
        "loyalty_tier" -> "unrated",
        "total_sessions" -> 0,
        "total_engagement_sec" -> 0,
        "txn_status" -> "unknown"
      ))
      .withColumn("txn_amount", coalesce($"txn_amount", lit(0.0)))
      .filter($"customer_id".isNotNull)
      .dropDuplicates("customer_id", "txn_id")
      .withColumn("engagement_score",
        round(($"total_engagement_sec" / 60.0) * (col("avg_pages_per_session") + lit(1)), 2))
      .withColumn("is_high_value",
        when($"txn_amount" > 5000, true).otherwise(false))
      .withColumn("load_timestamp", current_timestamp())
  }
}
