package spark.examples

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, SparkSession}
import spark.examples.Example17SerializationIssue.moviesFilePath

object Example17SerializationIssue extends App {

  val moviesFilePath = getClass().getResource("/movies.csv").getPath()

  val sparkSession = SparkSession.builder
    .appName("Simple Application")
    .master("local[2]")
    .getOrCreate()

  new Process().start(sparkSession, moviesFilePath)
}

class Process {

  def start(sparkSession: SparkSession, filePath: String): Unit = {

    import sparkSession.implicits._

    val moviesDataset: Dataset[Movie] = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv(moviesFilePath)
      .withColumnRenamed("movie_id", "movieId")
      .withColumnRenamed("movie_name", "movieName")
      .withColumn("year", 'year.cast(IntegerType))
      .as[Movie]
      .map(movie => renameMovie(movie)) // renameMovie is part of 'Process' class and it's not 'Serializable', this will produce a failure

    moviesDataset.show(false)
  }

  private def renameMovie(movie: Movie): Movie = {
    movie.movieName = movie.movieName + "!"
    movie
  }
}

