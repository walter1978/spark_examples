package spark.examples

import org.apache.spark.sql.functions.{collect_list,first}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, SparkSession}

object Example05DatasetJoin extends App {

  val moviesFilePath = getClass().getResource("/movies.csv").getPath()
  val actorsFilePath = getClass().getResource("/actors.csv").getPath()

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

  val actorsDataset: Dataset[Actor] = sparkSession.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv(actorsFilePath)
    .withColumnRenamed("movie_id", "movieId")
    .withColumnRenamed("actor_name", "actorName")
    .as[Actor]

  val moviesInfoDataset: Dataset[MovieInfo] = moviesDataset
    .joinWith(actorsDataset, moviesDataset("movieId") === actorsDataset("movieId"), joinType = "left_outer")
    .as[(Movie, Actor)]
    .groupBy($"_1.movieId")
    .agg(first($"_1.movieName") as "movieName", collect_list($"_2.actorName") as "actorNames")
    .as[MovieInfo]

  val moviesInfo: Seq[MovieInfo] = moviesInfoDataset.collect()

  moviesInfoDataset.show(false)
}


