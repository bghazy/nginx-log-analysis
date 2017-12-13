import Utilities._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object ServiceByBrowserAnalysisJob extends App {
  val parse_service_from_url: (Column) => Column = (x) => { split(x, "/")(2) }
  val ds = dataset.withColumn("service", parse_service_from_url(col("URL")))
  val byBrowserService = Window.partitionBy("UserAgent", "service")
  val result = ds.withColumn( "count", count(ds("remoteUser")).over(byBrowserService))
  result.select("UserAgent", "service", "count").distinct().take(10).foreach(u=>println(u))
}
