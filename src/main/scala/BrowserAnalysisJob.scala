import Utilities._
import org.apache.spark.sql.functions._

object BrowserAnalysisJob extends App {
  val result = dataset.groupBy("UserAgent").agg(count("remoteUser"))
  result.take(10).foreach(u=>println(u))
}
