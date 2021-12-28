package Platform

object DBHelper {
  def suppressLogs(params: List[String]): Unit = {
    // Levels: all, debug, error, fatal, info, off, trace, trace_int, warn
    import org.apache.log4j.{Level, Logger}
    params.foreach(Logger.getLogger(_).setLevel(Level.OFF))
  }
}
