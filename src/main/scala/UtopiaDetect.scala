package com.vincent.sparkhtx
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.expressions.Window

object UtopiaDetect {
  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println("Usage: UtopiaItemDetectionApp <inputPath1> <inputPath2> <outputPath> <topX>")
      System.exit(1)
    }

    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("UtopiaDetect")
      .getOrCreate()

    // Read input paths from command line arguments
    val inputPath1 = args(0) // Parquet File 1
    val inputPath2 = args(1) // Parquet File 2
    val outputPath = args(2) // Parquet File 3
    val topX = args(3).toInt // Top X items

    // Load Parquet files
    val datasetA: DataFrame = spark.read.parquet(inputPath1)
    val datasetB: DataFrame = spark.read.parquet(inputPath2)

    // Remove duplicate detection IDs from dataset A
    val uniqueDatasetA: DataFrame = datasetA
      .dropDuplicates("detection_oid")
      .select("geographical_location_oid", "item_name")

    // Join datasets based on geographical location OID
    val joinedData: DataFrame = datasetB
      .join(uniqueDatasetA, Seq("geographical_location_oid"), "inner")

    // Count item occurrences
    val itemCounts: DataFrame = joinedData
      .groupBy("geographical_location", "item_name")
      .count()
      .withColumn("item_rank", row_number().over(Window.partitionBy("geographical_location").orderBy(col("count").desc)))

    // Select top X items
    val topXItems: DataFrame = itemCounts.filter(col("item_rank") <= topX)

    // Write output to Parquet file
    topXItems
      .select("geographical_location", "item_rank", "item_name")
      .write
      .parquet(outputPath)

    // Stop Spark session
    spark.stop()
  }
}
