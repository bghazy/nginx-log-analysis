import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import Utilities._

object ServicesAnalysisJob extends App {

  val parse_service_from_url: (Column) => Column = (x) => { split(x, "/")(2) }
  val ds = dataset.withColumn("service", parse_service_from_url(col("URL")))

  val result = ds.groupBy("service").agg(count("remoteUser"))
  result.take(10).foreach(u=>println(u))

}
