package Platform

import org.apache.spark.sql.DataFrame
import Platform.DB
import org.apache.spark
object Analysis {

  def averageAlbumTrackLength(): Unit = {
    val spark = DB.getSparkSession()
    spark.sql("SELECT a.name, Round(AVG(t.duration_ms)/1000, 0) AS AverageLength FROM album a " +
      "JOIN album_tracks at ON a.id=at.id " +
      "JOIN track t on at.track_id = t.id GROUP BY a.name").show()
  }

  def printSimpleSchema(): Unit = {
    val spark = DB.getSparkSession()
    DB.tableNames.foreach{f =>
      println(f)
      spark.sqlContext.table(f).printSchema()
    }
  }

}
