package Platform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DB {
  private var sparkSession: SparkSession = null
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
    val scraperName = "smallest"

    val path = os.pwd/"spotifydata"/scraperName/"music_data"
//    val filenames = List("genre", "album", "album_artists", "album_tracks", "artist",
//      "artist_genres", "owner", "playlist", "playlist_tracks", "track", "track_artists")
    val filenames = List("genre")
//    println(filenames.forall(f => os.exists(path / s"$f.txt")))
//    println(path)
    genre()
    filenames.foreach(inputFileIntoTable(scraperName, _))
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
    spark.sql(s"SELECT * FROM $fileAndTableName").show()
  }
  def genre(): Unit = {
    val spark = getSparkSession()
    spark.sql("CREATE TABLE IF NOT EXISTS genre (" +
      "id INT," +
      "name VARCHAR(150)) " +
      "COMMENT 'Gives the full name of a genre' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "LINES TERMINATED BY '\\n'")
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


  def createTables(scraperName: String): Unit = {

    val path = os.pwd/"spotifydata"/scraperName/"music_data"/"genre.txt"
    val spark = getSparkSession()

    spark.sql("DROP table IF EXISTS genre")
    spark.sql("CREATE TABLE IF NOT EXISTS genre (" +
      "id INT," +
      "name VARCHAR(150)) " +
      "COMMENT 'Gives the full name of a genre' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "LINES TERMINATED BY '\\n'")
    spark.sql("LOAD DATA INPATH '/spotifydata/wall-e/music_data/genre.txt' OVERWRITE INTO TABLE genre")
    spark.sql("SELECT Count(*) AS TOTALCOUNT FROM genre").show()
//    spark.sql("SELECT Count(*) AS NumBranch2BevAFile FROM BevA WHERE BevA.BranchID='Branch2'").show()
    spark.sql("SELECT * FROM genre").show()
  }
}
