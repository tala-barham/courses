import org.apache.spark.sql.{DataFrame, SparkSession}
import scalaj.http.Http

object Api {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read API Data")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val apiUrl = "https://www.arbeitnow.com/api/job-board-api?fbclid=IwAR3_a5RTOEMp9gOZAoRZ04SS-TCH_NZaIG8U4IGD7FwVTkIYc2MDVQr-FUs"
    val response = Http(apiUrl).asString.body
    val apiDataFrame: DataFrame = spark.read.json(Seq(response).toDS())
    apiDataFrame.toJSON.collect().foreach(println)
    apiDataFrame.printSchema()
    spark.stop()
  }
}