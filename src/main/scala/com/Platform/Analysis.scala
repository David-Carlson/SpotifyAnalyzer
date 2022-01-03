package com.Platform

import org.apache.spark.sql.DataFrame
import com.Platform.DB
import com.Platform.DB.getSparkSession
import org.apache.spark
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext

import scala.collection.JavaConversions.collectionAsScalaIterable

object Analysis {
  def getArchiveStats(): Unit = {
    val spark = getSparkSession()
    import spark.implicits._
    val playlists = spark.sql("SELECT COUNT(DISTINCT(id)) trackCount FROM playlist").collect()(0).getLong(0)
    val albums = spark.sql("SELECT COUNT(*) trackCount FROM album").collect()(0).getLong(0)
    val artists = spark.sql("SELECT COUNT(*) trackCount FROM artist").collect()(0).getLong(0)
    val tracks = spark.sql("SELECT COUNT(*) trackCount FROM track").collect()(0).getLong(0)
    val genres = spark.sql("SELECT COUNT(*) trackCount FROM genre").collect()(0).getLong(0)
    println("Represented in the loaded archive: \n")
    println(s"\tPlaylists: $playlists")
    println(s"\talbums: $albums")
    println(s"\tartists: $artists")
    println(s"\ttracks: $tracks")
    println(s"\tgenres: $genres")
    println()
  }

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

  def getTrackSuggestions(user_id: String): Unit = {
    val spark = DB.getSparkSession()
    println(s"Finding tracks to fill your musical gaps")
    val genres = s"(SELECT g.id, g.name" +
      s" FROM playlist p " +
      s" JOIN playlist_tracks pt ON p.id = pt.id " +
      s" JOIN track_artists ta ON pt.track_id = ta.id " +
      s" JOIN artist_genres ag ON ta.artist_id = ag.id " +
      s" JOIN genre g on ag.genre_id = g.id " +
      s" WHERE p.owner_id !='$user_id' " +
      s" GROUP BY g.id, g.name" +
      s" ORDER BY COUNT(*) DESC " +
      s" LIMIT 10)"
    val df = spark.sql(genres)
    df.createOrReplaceTempView("missing_genres")
    spark.sql("SELECT t.name, a.name artistName, mg.name genreName FROM missing_genres mg" +
      " JOIN artist_genres ag ON mg.id = ag.genre_id" +
      " JOIN track_artists ta ON ag.id = ta.artist_id" +
      " JOIN track t on ta.id = t.id" +
      " JOIN artist a on ta.artist_id = a.id" +
      " WHERE t.popularity > 0" +
      " ORDER BY RAND()" +
      " LIMIT 10").show(truncate = false)
  }
//  def profanityByUser(): Unit = {
//
//  }

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
      s" ORDER BY Round(AVG(t.popularity), 0) DESC LIMIT 10").show(truncate = false)
  }

  def printSimpleSchema(): Unit = {
    val spark = DB.getSparkSession()
    TableInfo.tableNames.foreach{f =>
      println(f)
      spark.sqlContext.table(f).printSchema()
    }
  }

}
