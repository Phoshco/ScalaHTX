// UtopiaDetect.scala

package com.vincent.sparkhtx

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object UtopiaDetect {

  // Initialize Spark Session
  val spark: SparkSession = SparkSession.builder()
    .appName("UtopiaDetect")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    if (!checkArgs(args)){
      System.exit(1)
    }

    val inputPath1 = args(0)
    val inputPath2 = args(1)
    val outputPath = args(2)
    val topX = args(3).toInt

    // Read Parquet files as DataFrames and convert to RDDs
    val datasetA = spark.read.parquet(inputPath1)
    val rddA = datasetA.rdd
    val rddB = spark.read.parquet(inputPath2).rdd

    // Identify the skewed geographical location (if required)
    val skewedLocation = 12345 // Replace with the actual skewed location OID
    // Filter datasetA to exclude the skewed geographical location
    val filteredDatasetA: DataFrame = datasetA.filter(col("geographical_location_oid") =!= skewedLocation)

    val itemCountsRDD = removeDuplicatesAndMap(rddA)
    val joinedRDD = joinLocations(rddB, itemCountsRDD)
    val rankedItemsRDD = groupRank(joinedRDD)
    val topXItemsRDD = getTopX(rankedItemsRDD, topX)

    // Define schema for DataFrame
    val schema = StructType(Seq(
      StructField("geographical_location", StringType, nullable = false),
      StructField("item_rank", IntegerType, nullable = false),
      StructField("item_name", StringType, nullable = true)
    ))

    // Convert RDD to DataFrame and save as Parquet
    val rankedItemsDF = spark.createDataFrame(topXItemsRDD.map {
      case (_, (locationName, itemName, itemRank)) =>
        Row(locationName, itemRank, itemName)
    }, schema)

    rankedItemsDF.write.parquet(outputPath)

    // Stop Spark session
    spark.stop()
  }

  def checkArgs(args: Array[String]): Boolean = {
    if (args.length != 4) {
      println("Usage: UtopiaDetect <inputPath1> <inputPath2> <outputPath> <topX>")
      false
    } else {
      true
    }
  }
  def removeDuplicatesAndMap(rddA: org.apache.spark.rdd.RDD[Row]): org.apache.spark.rdd.RDD[(Long, String)] = {
    rddA
      .map { row => (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("item_name")) } // Map to (locationId, itemName) directly
      .distinct() // Remove duplicates based on (location oid, item name)
  }

  def joinLocations(rddB: org.apache.spark.rdd.RDD[Row], rddA: org.apache.spark.rdd.RDD[(Long, String)]): org.apache.spark.rdd.RDD[(Long, (String, String))] = {
    rddA
      .cogroup(rddB.map(row => row.getAs[Long]("geographical_location_oid") -> row.getAs[String]("geographical_location"))) // Cogroup by locationId
      .filter { case (locationId, (itemAndLocations, _)) => itemAndLocations.nonEmpty } // Filter locations with detections
      .flatMap { case (locationId, (itemAndLocations, locationOption)) =>
        itemAndLocations.map(itemName => (locationId, (locationOption.head, itemName)))
      }
  }

  def groupRank(joinedRDD: RDD[(Long, (String, String))]): RDD[(Long, (String, String, Int))] = {
    joinedRDD
      .map { case (locationId, (locationName, itemName)) => (locationId, (locationName, itemName, 1)) } // Map to (locationId, (locationName, itemName, count))
      .groupByKey() // Group by locationId
      .mapValues { items =>  // Reduce by key to get item counts
        items.reduce { case ((locationName1, itemName1, count1), (locationName2, itemName2, count2)) =>
          (locationName1, itemName1, count1 + count2)
        }
      }
  }

  def sortItemsDescendingByCount(partition: Iterator[(Long, (String, String, Int))]): Iterator[(Long, (String, String, Int))] = {
    partition.toList.sortBy(item => item._2._3)(Ordering[Int].reverse).iterator // Sort by count descending
  }

  def takeTopXAndZipWithIndex(sortedItems: Iterator[(Long, (String, String, Int))], topX: Int): Iterator[(Long, (String, String, Int))] = {
    sortedItems.take(topX).zipWithIndex.map { // Take topX and zip with index for rank
      case ((locationId, (locationName, itemName, count)), rank) => (locationId, (locationName, itemName, rank + 1))
    }
  }

  def getTopX(rankedItemsRDD: RDD[(Long, (String, String, Int))], topX: Int): RDD[(Long, (String, String, Int))] = {
    rankedItemsRDD.mapPartitions { partition => // MapPartitions for efficiency with shuffle within partition
      takeTopXAndZipWithIndex(sortItemsDescendingByCount(partition), topX)
    }
  }
}
