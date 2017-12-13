import java.util.regex.Pattern
import Model._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col

object Utilities {

  lazy val sparkSession:SparkSession = SparkSession.builder
    .master("local")
    .appName("nginx_log_analysis")
    .getOrCreate()

  var path:String = getClass.getResource("nginx_access.log").getPath
  import sparkSession.implicits._
  val url_contains_service: (Column) => Column = (x) => { x.like("/service/%")}
  lazy val dataset:Dataset[NginxLogRecord] = sparkSession.read.text(path).map{
    record => parseNginxLogRecord(record)
  }.where(url_contains_service(col("URL")))

  def parseNginxLogRecord(row: Row): NginxLogRecord = {
    val r = Pattern.compile("\"").matcher(row.toString()).replaceAll("")
    val record =  Pattern.compile(" ").split(r)
        NginxLogRecord(
          record(0),
          record(1),
          record(3),
          record(5),
          record(6),
          record(7),
          record(8),
          record(9),
          record(10),
          record(11),
          record(12),
          if (record.length>13) record(13) else "",
          if (record.length>14) record(14) else ""
        )
  }
}
