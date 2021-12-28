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
