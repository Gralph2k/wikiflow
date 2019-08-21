package com.renarde.wikiflow.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


object AnalyticsReader extends App with LazyLogging {

  val appName: String = "analytics-reader-example"

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  logger.info("Initializing analiytics reader")

  spark.sparkContext.setLogLevel("WARN")

  spark.read.parquet("/storage/analytics-consumer/output")
    .groupBy("type")
    .agg(sum($"cnt").as("cnt")
        ,min($"window.start").as("min window.start")
        ,max($"window.start").as("max window.start")
        ,max($"load_dttm").as("max load_dttm")
     )
    .orderBy($"cnt".desc)
    .show(false)
}
