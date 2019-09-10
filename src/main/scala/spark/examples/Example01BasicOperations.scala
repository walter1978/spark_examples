package spark.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Example01BasicOperations extends App {

  val sparkSession = SparkSession.builder
    .appName("Simple Application")
    .master("local[2]")
    .getOrCreate()

  val data = Array(1, 2, 3, 4, 5)
  val distributedData: RDD[Int] = sparkSession.sparkContext.parallelize(data)

  // Transformations ------------------------------------------------------------------------------------------

  // filtering
  val rdd1: RDD[Int] = distributedData.filter(data => data > 1)

  val rdd1Result: Array[Int] = rdd1.collect()

  // mapping
  val rdd2: RDD[Int] = distributedData.map(data => data + 100)
  val rdd2Result: Array[Int] = rdd2.collect()

  // Operations ----------------------------------------------------------------------------------------------

  // reduce - aggregates the elements of the RDD using a function
  val reduceResult: Int = distributedData.reduce((a, b) => a + b)

  // collect - return all the elements of the RDD as an array at the driver program
  val collectResult: Array[Int] = distributedData.collect()

  // count - return the number of elements in the RDD
  val countResult: Long = distributedData.count()

  // RDD Persistence  ----------------------------------------------------------------------------------------

  val cachedData: RDD[Int] = distributedData.map(data => data + 100).cache() // persists the data in node's memory
  val r1: Long = cachedData.filter(data => data < 5).count()
  val r2: Long = cachedData.filter(data => data >= 5).count()
}
