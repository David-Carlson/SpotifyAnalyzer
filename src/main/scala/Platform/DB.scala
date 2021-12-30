package Platform

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DB {
  private var sparkSession: SparkSession = null
  val tableNames = List("genre", "album", "artist", "owner", "playlist", "track", "album_artists",
    "album_tracks", "artist_genres", "playlist_tracks", "track_artists")

  def getSparkSession(): SparkSession = {
    if (sparkSession == null) {
      println("Creating SparkSession")
      DBHelper.suppressLogs(List("org", "akka"))
      System.setProperty("hadoop.home.dir", "C:\\hadoop")
      sparkSession = SparkSession
        .builder
        .appName("Spotify Analyzer")
        .config("spark.master", "local")
        .enableHiveSupport()
        .getOrCreate()
      sparkSession.sparkContext.setLogLevel("ERROR")
    }
    sparkSession
  }
  def main(args: Array[String]): Unit = {
    val crawlerName = "smallest"
    Analysis.averageAlbumTrackLength()
//    setupDatabase(crawlerName)
//    sparkTest()
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
    val filenames = tableNames
    filenames.foreach{f =>
      println(f)
      spark.sqlContext.table(f).printSchema()
    }
//    filenames.foreach(dropTable)
//    createAllTables()
//    if (canLoadTables(crawlerName, filenames))
//      filenames.foreach(inputFileIntoTable(crawlerName, _))
  }

  def canLoadTables(crawlerName: String, fileAndTableNames: List[String]): Boolean = {
    fileAndTableNames.forall(f => os.exists(os.pwd/"spotifydata"/crawlerName/"music_data"/(f + ".txt")))
  }

  def createAllTables(): Unit = {
    val spark = getSparkSession()
    TableDefinitions.tableSchemas.foreach(spark.sql)
  }

  def dropTable(fileAndTableName: String): Unit = {
    val spark = getSparkSession()
    spark.sql(s"DROP table IF EXISTS $fileAndTableName")
  }

  def inputFileIntoTable(crawlerName: String, fileAndTableName: String): Unit = {
    val path = os.pwd/"spotifydata"/crawlerName/"music_data"/(fileAndTableName + ".txt")
    val inputStr = os.read.lines.stream(path)
      .filter(_.nonEmpty)
      .map(_.split("\\|").map(_.trim).mkString(","))
      .map(l => s"($l)")
      .mkString(", ")
    val spark = getSparkSession()
    spark.sql(s"INSERT INTO TABLE $fileAndTableName VALUES " + inputStr)
//    spark.sql(s"SELECT * FROM $fileAndTableName").show()
//    spark.sql(s"SELECT COUNT(*) FROM $fileAndTableName").show()
  }


  def test(): Unit = {
    val spark = getSparkSession()

    spark.sql("DROP table IF EXISTS BevA")
    spark.sql("create table IF NOT EXISTS BevA(Beverage String,BranchID String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'Bev_BranchA.txt' INTO TABLE BevA")
    spark.sql("SELECT Count(*) AS TOTALCOUNT FROM BevA").show()
    spark.sql("SELECT Count(*) AS NumBranch2BevAFile FROM BevA WHERE BevA.BranchID='Branch2'").show()
    spark.sql("SELECT * FROM BevA").show()
  }
}
