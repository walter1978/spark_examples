package spark.examples

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Example07SQL extends App {

  val moviesFilePath = getClass().getResource("/movies.csv").getPath()
  val actorsFilePath = getClass().getResource("/actors.csv").getPath()

  val sparkSession = SparkSession.builder
    .appName("Simple Application")
    .master("local[2]")
    .getOrCreate()

  val moviesDataset: DataFrame = sparkSession.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv(moviesFilePath)

  moviesDataset.createOrReplaceTempView("movies")

  val actorsDataset: DataFrame = sparkSession.read
    .option("header", "true")
    .option("delimiter", ",")
      .csv(actorsFilePath)

  actorsDataset.createOrReplaceTempView("actors")

  import sparkSession.implicits._

  val moviesInfoDataset: Dataset[MovieInfo] = sparkSession.sql(
    "SELECT " +
      "movies.movie_id as movieId, " +
      "first(movie_name) as movieName, " +
      "cast(first(movies.year) as int) as year, " +
      "collect_list(actors.actor_name) as actorNames " +
      "FROM movies " +
      "LEFT JOIN actors on movies.movie_id = actors.movie_id " +
      "GROUP BY movies.movie_id"
  ).as[MovieInfo]

  val moviesInfo: Seq[MovieInfo] = moviesInfoDataset.collect()

  moviesInfoDataset.show(false)
}


