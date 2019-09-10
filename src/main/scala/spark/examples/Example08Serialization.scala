package spark.examples

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, SparkSession}

object Example16Serialization extends App {

  val moviesFilePath = getClass().getResource("/movies.csv").getPath()

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
    // lambda expressions can be serialized and sent to worker nodes
    .map(movie => {
        movie.movieName = movie.movieName + "!"
        movie
      })
    // objects can be used on worker nodes
    .map(movie => ExampleObject.renameMovie(movie))
    // instances of classes extending 'Serializable' can be serialized and sent to worker nodes
    .map(movie => new ExampleSerializable().renameMovie(movie))

  moviesDataset.show(false)
}

object ExampleObject {

  def renameMovie(movie: Movie): Movie = {
    movie.movieName = movie.movieName + "!"
    movie
  }
}

class ExampleSerializable extends Serializable {

  def renameMovie(movie: Movie): Movie = {
    movie.movieName = movie.movieName + "!"
    movie
  }
}
