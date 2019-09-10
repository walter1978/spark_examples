package spark.examples

import org.apache.spark.sql.functions.{collect_list, first}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Example04DataFrameJoin extends App {

  val moviesFilePath = getClass().getResource("/movies.csv").getPath()
  val actorsFilePath = getClass().getResource("/actors.csv").getPath()

  val sparkSession = SparkSession.builder
    .appName("Simple Application")
    .master("local[2]")
    .getOrCreate()

  import sparkSession.implicits._

  val moviesDataFrame: DataFrame = sparkSession.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv(moviesFilePath)

  val actorsDataFrame: DataFrame = sparkSession.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv(actorsFilePath)

  val moviesInfoDataframe: DataFrame = moviesDataFrame
    .join(actorsDataFrame, Seq("movie_id"), "fullouter")
    .groupBy("movie_id")
    .agg(first($"movie_name"), first($"year"), collect_list($"actor_name"))

  val rows: Array[Row] = moviesInfoDataframe.collect()

  moviesInfoDataframe.show(false)
}


