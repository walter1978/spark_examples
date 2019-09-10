package spark.examples

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, SparkSession}

object Example06UDF extends App {

  val moviesFilePath = getClass().getResource("/movies.csv").getPath()

  val sparkSession = SparkSession.builder
    .appName("Simple Application")
    .master("local[2]")
    .getOrCreate()

  import sparkSession.implicits._

  val function = (value: String) => "!!" + value + "!!"
  val userDefinedFunction: UserDefinedFunction = udf(function)

  val moviesDataset: Dataset[Movie] = sparkSession.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv(moviesFilePath)
    .withColumnRenamed("movie_id", "movieId")
    .withColumnRenamed("movie_name", "movieName")
    .withColumn("year", 'year.cast(IntegerType))
    .withColumn("movieName", userDefinedFunction('movieName))
    .as[Movie]

  val movies: Array[Movie] = moviesDataset.collect()

  moviesDataset.show(false)
}


