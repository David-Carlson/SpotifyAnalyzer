package Platform

import org.apache.spark.sql.DataFrame
import Platform.DB
import org.apache.spark
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext

object Analysis {

  def averageAlbumTrackLength(): Unit = {
    val spark = DB.getSparkSession()
    val sql = "SELECT a.name, Round(AVG(t.duration_ms)/1000, 0) AS AverageLength, Round(variance(t.duration_ms/1000), 0) Variance " +
      " FROM album a " +
      " JOIN album_tracks at ON a.id=at.id " +
      " JOIN track t on at.track_id = t.id GROUP BY a.name" +
      " ORDER BY AVG(t.duration_ms) "
    println("Longest albums: ")
    spark.sql(sql +"DESC LIMIT 10").show(truncate = false)
    println("Shortest albums: ")
    spark.sql(sql +"ASC LIMIT 10").show(truncate = false)
  }

  def getUserGenres(user_id: String): Unit = {
    val spark = DB.getSparkSession()
    println(s"The top genres that $user_id enjoys: ")
    spark.sql(s"SELECT g.name, COUNT(*) as GenreOccurrences " +
      s" FROM playlist p " +
      s" JOIN playlist_tracks pt ON p.id = pt.id " +
      s" JOIN track_artists ta ON pt.track_id = ta.id " +
      s" JOIN artist_genres ag ON ta.artist_id = ag.id " +
      s" JOIN genre g on ag.genre_id = g.id " +
      s" WHERE p.owner_id = '$user_id' " +
      s" GROUP BY g.name " +
      s" ORDER BY COUNT(*) DESC " +
      s" LIMIT 10").show(10, truncate = false)
  }
  def getMissingUserGenres(user_id: String): Unit = {
    val spark = DB.getSparkSession()
    println(s"The top genres that $user_id is missing out on...")
    spark.sql(s"SELECT g.name, COUNT(*) as GenreOccurrences " +
      s" FROM playlist p " +
      s" JOIN playlist_tracks pt ON p.id = pt.id " +
      s" JOIN track_artists ta ON pt.track_id = ta.id " +
      s" JOIN artist_genres ag ON ta.artist_id = ag.id " +
      s" JOIN genre g on ag.genre_id = g.id " +
      s" WHERE p.owner_id !='$user_id' " +
      s" GROUP BY g.name " +
      s" ORDER BY COUNT(*) DESC " +
      s" LIMIT 10").show(10, truncate = false)
  }

  def getAvgTrackPopularityByUser(): Unit = {
    val spark = DB.getSparkSession()
    println(s"The average popularity of songs by user...")
    spark.sql(s"SELECT o.name, Round(AVG(t.popularity), 0) AvgPopularity " +
      s" FROM playlist p " +
      s" JOIN playlist_tracks pt ON p.id = pt.id " +
      s" JOIN track t ON pt.track_id = t.id " +
      s" Join owner o ON o.id = p.owner_id " +
      s" WHERE t.popularity > 0 " +
      s" GROUP BY o.name").show(truncate = false)
  }

  def getAvgTrackPopularityByPlaylist(user: String = ""): Unit = {
    val spark = DB.getSparkSession()
    val where = if (user.isEmpty) "" else s"AND p.owner_id = '$user'"
    println(s"The average popularity of songs by playlist...")
    spark.sql(s"SELECT p.name, o.name, Round(AVG(t.popularity), 0) AvgPopularity " +
      s" FROM playlist p " +
      s" JOIN playlist_tracks pt ON p.id = pt.id " +
      s" JOIN track t ON pt.track_id = t.id " +
      s" Join owner o ON o.id = p.owner_id " +
      s" WHERE t.popularity > 0 " + where +
      s" GROUP BY p.name, o.name " +
      s" ORDER BY Round(AVG(t.popularity), 0) DESC LIMIT 20").show(truncate = false)
  }

  def getPlaylistExplosivity(user: String = ""): Unit = {
    val spark = DB.getSparkSession()
    val where = if (user.isEmpty) "" else s"AND p.owner_id = '$user'"
    println(s"The average popularity of songs by playlist...")
    spark.sql(s"SELECT p.name, o.name, Round(AVG(t.popularity), 0) AvgPopularity FROM " +
      s" playlist p " +
      s" JOIN playlist_tracks pt ON p.id = pt.id " +
      s" JOIN track t ON pt.track_id = t.id " +
      s" Join owner o ON o.id = p.owner_id " +
      s" WHERE t.popularity > 0 " + where +
      s" GROUP BY p.name, o.name " +
      s" ORDER BY Round(AVG(t.popularity), 0) DESC LIMIT 20").show(truncate = false)
  }

  def printSimpleSchema(): Unit = {
    val spark = DB.getSparkSession()
    TableInfo.tableNames.foreach{f =>
      println(f)
      spark.sqlContext.table(f).printSchema()
    }
  }

}
