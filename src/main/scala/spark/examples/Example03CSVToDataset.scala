package spark.examples

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, SparkSession}

object Example03CSVToDataset extends App {

  val moviesFilePath = getClass().getResource("/movies.csv").getPath()  // structured data with header

  val sparkSession = SparkSession.builder
    .appName("Simple Application")
    .master("local[2]")
    .getOrCreate()

  import sparkSession.implicits._

  val moviesDataset: Dataset[Movie] = sparkSession.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv(moviesFilePath)
    .withColumnRenamed("movie_id", "movieId")
    .withColumnRenamed("movie_name", "movieName")
    .withColumn("year", 'year.cast(IntegerType))
    .as[Movie]

  val movies: Array[Movie] = moviesDataset.collect()

  moviesDataset.show(false)
}


