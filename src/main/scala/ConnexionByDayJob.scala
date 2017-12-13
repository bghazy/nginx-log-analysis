import Utilities._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object ConnexionByDayJob extends App {

  val parse_date_from_datetime: (Column) => Column = (x) => { split(x, ":")(0).substr(2,11) }
  val ds = dataset.withColumn("date", parse_date_from_datetime(col("dateTime")))
  val result = ds.groupBy("date").agg(count("remoteUser") as "count").orderBy("count")
  result.take(10).foreach(u=>println(u))

}
