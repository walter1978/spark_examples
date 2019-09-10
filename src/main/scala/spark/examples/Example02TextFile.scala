package spark.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Example02TextFile extends App {

  val statesFilePath = getClass().getResource("/states.txt").getPath()  // structured data without header

  val sparkSession = SparkSession.builder
    .appName("Simple Application")
    .master("local[2]")
    .getOrCreate()

  // RDD ---------------------------------------------------------------------------------------------------------

  val statesRDD: RDD[State] = sparkSession.sparkContext
    .textFile(statesFilePath)
    .map(line => {
      val parts = line.split("/")
      State(parts(0), parts(1))
    })

  val statesFromRDD: Array[State] = statesRDD.collect()

  // DataFrame ---------------------------------------------------------------------------------------------------

  val statesSchema = StructType(List(
    StructField("name", StringType),
    StructField("abbreviation", StringType)
  ))

  val statesDataFrame: DataFrame = sparkSession.read
    .schema(statesSchema)
    .option("header", "false")
    .option("delimiter", "/")
    .csv(statesFilePath)

  val statesFromDataFrame: Array[Row] = statesDataFrame.collect()

  // Dataset -----------------------------------------------------------------------------------------------------

  import sparkSession.implicits._

  val statesDataset: Dataset[State] = sparkSession.read
    .schema(statesSchema)
    .option("header", "false")
    .option("delimiter", "/")
    .csv(statesFilePath)
    .as[State]

  val statesFromDataset: Array[State] = statesDataset.collect()
}
