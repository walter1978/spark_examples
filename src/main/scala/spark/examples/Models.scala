package spark.examples

case class Movie(movieId: String, var movieName: String, year: Int)

case class Actor(movieId: String, actorName: String)

case class MovieInfo(movieId: String, movieName: String, actorNames: Seq[String])

case class State(name: String, abbreviation: String)
