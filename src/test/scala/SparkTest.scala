package com.vincent.sparkhtx
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SparkTest extends AnyFunSuite with BeforeAndAfter {

  private var spark: SparkSession = _

  before {
    spark = SparkSession.builder().appName("test").getOrCreate()
  }
  after {
    spark.stop()
  }

  test("exit with an error message for invalid arguments") {
    val invalidArgs = Array.empty[String]
    val expectedOut = UtopiaDetect.checkArgs(invalidArgs)
    assert(!expectedOut)
  }

  test("removeDuplicatesAndMap: removes duplicates") {
    val inputRDD = spark.sparkContext.parallelize(
      Seq(Row(1L, "item1"), Row(1L, "item1"), Row(2L, "item2"))
    )
    val expectedOutput = spark.sparkContext.parallelize(
      Seq((1L, "item1"), (2L, "item2"))
    )
    val actualOutput = UtopiaDetect.removeDuplicatesAndMap(inputRDD)
    assert(actualOutput.collect().toSet === expectedOutput.collect().toSet)
  }

  test("removeDuplicatesAndMap: handles empty data") {
    val inputRDD = spark.sparkContext.emptyRDD[Row]
    val expectedOutput = spark.sparkContext.emptyRDD[(Long, String)]
    val actualOutput = UtopiaDetect.removeDuplicatesAndMap(inputRDD)
    assert(actualOutput.isEmpty())
  }

  test("joinLocations: matches location and item names") {
    val rddA = spark.sparkContext.parallelize(Seq(Row(1L, "item1")))
    val rddB = spark.sparkContext.parallelize(Seq(Row(1L, "location1")))
    val expectedOutput = spark.sparkContext.parallelize(Seq((1L, ("location1", "item1"))))
    val actualOutput = UtopiaDetect.joinLocations(rddB, UtopiaDetect.removeDuplicatesAndMap(rddA))
    assert(actualOutput.collect().toSet === expectedOutput.collect().toSet)
  }

  test("joinLocations: handles mismatched locationIds") {
    val rddA = spark.sparkContext.parallelize(Seq(Row(1L, "item1")))
    val rddB = spark.sparkContext.parallelize(Seq(Row(2L, "location1")))
    val actualOutput = UtopiaDetect.joinLocations(rddB, UtopiaDetect.removeDuplicatesAndMap(rddA))
    assert(actualOutput.isEmpty())
  }

  test("joinLocations: handles missing location in rddB") {
    val rddA = spark.sparkContext.parallelize(Seq(Row(2L, "item1")))
    val rddB = spark.sparkContext.parallelize(Seq(Row(1L, "location1")))
    val actualOutput = UtopiaDetect.joinLocations(rddB, UtopiaDetect.removeDuplicatesAndMap(rddA))
    assert(actualOutput.isEmpty())
  }

  test("groupRank: groups and sums item counts") {
    val inputRDD = spark.sparkContext.parallelize(Seq(Row(1L, "item1"), Row(1L, "item2"), Row(2L, "item1")))
    val expectedOutput = spark.sparkContext.parallelize(Seq((1L, ("", "item1", 2)), (2L, ("", "item1", 1))))
    val actualOutput = UtopiaDetect.groupRank(UtopiaDetect.joinLocations(spark.sparkContext.emptyRDD[Row], UtopiaDetect.removeDuplicatesAndMap(inputRDD)))
    assert(actualOutput.collect().toSet === expectedOutput.collect().toSet)
  }

  test("groupRank: handles empty data") {
    val inputRDD = spark.sparkContext.emptyRDD[Row]
    val actualOutput = UtopiaDetect.groupRank(UtopiaDetect.joinLocations(spark.sparkContext.emptyRDD[Row], UtopiaDetect.removeDuplicatesAndMap(inputRDD)))
    assert(actualOutput.isEmpty())
  }

}
