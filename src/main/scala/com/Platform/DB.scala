package com.Platform

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Encoders, SparkSession}
import com.Platform.TableInfo._
import com.Platform.PasswordHash.validatePassword
import com.Platform.RowObjects.UserInfo

object DB {
  private var sparkSession: SparkSession = null


  def getSparkSession(): SparkSession = {
    if (sparkSession == null) {
      println("Creating SparkSession")
      suppressLogs(List("org", "akka"))
      System.setProperty("hadoop.home.dir", "C:\\hadoop")
      sparkSession = SparkSession
        .builder
        .appName("Spotify Analyzer")
        .config("spark.master", "local")
        .enableHiveSupport()
        .getOrCreate()
      sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      sparkSession.sparkContext.setLogLevel("ERROR")
    }
    sparkSession
  }
  def main(args: Array[String]): Unit = {
    val crawlerName = "large-database"

    val user = validateLogin("doctorsalt", "doctorsalts")
    if (user.isDefined)
      println(user)

//    setupDatabase(crawlerName)

//    println(validatePassword("admin", res(1)))
//    println(spark.sql("SELECT * FROM user_password ").show())
//    val paths = os.pwd / "spotifydata"
//    println(paths.baseName)
//    val startTime = System.nanoTime()
//

//    Analysis.getAvgTrackPopularityByUser()
//    (1 to 10).foreach(_ => Analysis.getAvgTrackPopularityByPlaylist("doctorsalt"))
//

//    Analysis.averageAlbumTrackLength()

//    val endTime  = (System.nanoTime()- startTime) / 1e9d
//    println(s"$endTime seconds")
//    sparkTest()
  }

  def fileStuff(): Unit = {
    os.list(os.pwd / "spotifydata").foreach(p => println(p.baseName))
  }

  def sparkTest(): Unit = {
    val spark = getSparkSession()
    val album_tracks = spark.sqlContext.table("album_tracks")
    val album = spark.sqlContext.table("album")
    val track = spark.sqlContext.table("track")
//    album
//      .join(album_tracks, "id")
//      .join(track, album_tracks("track_id") === track("id"))
//      .show()
    spark.sql("SELECT a.name, a.tracks, COUNT(*) FROM album a JOIN album_tracks at ON a.id=at.id " +
      "JOIN track t on t.id=at.track_id GROUP BY a.name, a.tracks").show()


//    df.select("name", "track_number").filter("track_number > 30").show()
//    println(df.filter("track_number > 30").count())
    album.printSchema()
  }

  def setupDatabase(crawlerName: String): Unit = {
    // Add other tables, login tables
    val spark = getSparkSession()

    tableNames.foreach(dropTable)
    createAllTables()
    inputFileIntoPartitionTable(crawlerName, "playlist")

    if (canLoadTables(crawlerName, tableNames)) {
      simpleTables.foreach(inputFileIntoTable(crawlerName, _))
      partitionTables.foreach(inputFileIntoPartitionTable(crawlerName, _))
    }
  }

  def printSimpleSchemas(): Unit = {
    val spark = getSparkSession()
    tableNames.foreach{f =>
      println(f)
      spark.sqlContext.table(f).printSchema()
    }
  }

  def canLoadTables(crawlerName: String, fileAndTableNames: List[String]): Boolean = {
    fileAndTableNames.forall(f => os.exists(os.pwd/"spotifydata"/crawlerName/"music_data"/(f + ".txt")))
  }

  def createAllTables(): Unit = {
    val spark = getSparkSession()
    tableSchemas.foreach(spark.sql)
  }

  def dropTable(fileAndTableName: String): Unit = {
    val spark = getSparkSession()
    spark.sql(s"DROP table IF EXISTS $fileAndTableName")
  }

  def inputFileIntoTable(crawlerName: String, fileAndTableName: String): Unit = {
    try {
      val path = os.pwd/"spotifydata"/crawlerName/"music_data"/(fileAndTableName + ".txt")
      val inputStr = os.read.lines.stream(path)
        .filter(_.nonEmpty)
        .map(_.split("\\|").map(_.trim).mkString(","))
        .map(l => s"($l)")
        .mkString(", ")
      val spark = getSparkSession()
      spark.sql(s"INSERT INTO TABLE $fileAndTableName VALUES " + inputStr)
    }
    catch {
      case ex: Throwable => println(s"simpleTable $fileAndTableName failed: ")
    }
  }

  def inputFileIntoPartitionTable(crawlerName: String, tableName: String): Unit = {
    val path = os.pwd/"spotifydata"/crawlerName/"music_data"/(tableName + ".txt")
    val spark = getSparkSession()
    for (row <- os.read.lines.stream(path) if row.nonEmpty) {
      val cols = row.split("\\|").map(_.trim)
      val part = cols(partitionIdx(tableName))
      val rest = cols.filter(_ != part).mkString(", ")
      val sql = getPartitionInsertHeader(tableName, part) + rest
      spark.sql(sql)
    }
  }
  def getPartitionInsertHeader(tableName: String, partition: String): String = {
    s"INSERT INTO $tableName PARTITION(${partitionName(tableName)}=$partition) SELECT "
  }

  def validateLogin(username: String, givenPassword: String): Option[UserInfo] = {
    val spark = getSparkSession()
    // TODO REMOVE
    spark.sql("SELECT * FROM user_password").show(20)

    import spark.implicits._
    val res = spark.sql(s"SELECT * FROM user_password where id='$username'")
    if (res.isEmpty)
      None
    else {
      val user = res.as[UserInfo].head
      if (PasswordHash.validatePassword(givenPassword, user.password))
        Some(user)
      else
        None
    }
  }

  def usernameIsFree(username: String): Boolean = {
    val spark = getSparkSession()
    spark.sql(s"SELECT * FROM user_password where id='$username'").isEmpty
  }

  def createUser(username: String, password: String): Option[UserInfo] = {
    val spark = getSparkSession()
    val hash = PasswordHash.createSaltedHash(password)
    spark.sql(s"INSERT INTO TABLE user_password VALUES ('$username', '$hash', false)")
    validateLogin(username, password)
  }

  def updatePassword(username: String, password: String): Option[UserInfo] = {
    val spark = getSparkSession()
    val hash = PasswordHash.createSaltedHash(password)
    spark.sql(s"UPDATE user_password SET password='$hash' WHERE username='$username")
    validateLogin(username, password)
  }


  def suppressLogs(params: List[String]): Unit = {
    // Levels: all, debug, error, fatal, info, off, trace, trace_int, warn
    import org.apache.log4j.{Level, Logger}
    params.foreach(Logger.getLogger(_).setLevel(Level.OFF))
  }
}
