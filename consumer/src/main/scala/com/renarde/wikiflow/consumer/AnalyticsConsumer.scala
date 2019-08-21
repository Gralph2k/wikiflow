package com.renarde.wikiflow.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



object AnalyticsConsumer extends App with LazyLogging {

  val appName: String = "analytics-consumer-example"

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  logger.info("Initializing analytics consumer")

  spark.sparkContext.setLogLevel("WARN")

  val inputStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "wikiflow-topic")
    .option("startingOffsets", "earliest")
    .load()


  val preparedDS = inputStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

  val rawData = preparedDS.filter($"value".isNotNull)

  val expectedSchema = new StructType()
    .add(StructField("bot", BooleanType))
    .add("timestamp", LongType)
    .add("type", StringType)

  val parsedData = rawData.select(from_json($"value", expectedSchema).as("data")).select("data.*")

  // please edit the code below
  val transformedStream: DataFrame = parsedData
    .filter("!bot")
    .withColumn("timestamp", ($"timestamp").cast(TimestampType))
    .withWatermark("timestamp", "30 seconds")
    .groupBy(
      window($"timestamp", "30 seconds"),
      $"type")
    .agg(count($"*").as("cnt"))
    .withColumn("load_dttm", expr("CURRENT_TIMESTAMP()"))


  val output = transformedStream.writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "/storage/analytics-consumer/checkpoints")
    .start("/storage/analytics-consumer/output")

  spark.streams.awaitAnyTermination()
}
