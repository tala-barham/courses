import org.apache.spark.sql.SparkSession
import java.util.Properties
import java.io.FileInputStream

object Coursera {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark")
      .config("spark.master", "local")
      .master("local[*]")
      .getOrCreate()

    // Load Snowflake connection details from properties file
    val props = new Properties()
    props.load(new FileInputStream("snowflake.properties"))

    val snowflake = Map(
      "sfurl" -> props.getProperty("sfurl"),
      "sfuser" -> props.getProperty("sfuser"),
      "sfpassword" -> props.getProperty("sfpassword"),
      "sfrole" -> props.getProperty("sfrole"),
      "sfwarehouse" -> props.getProperty("sfwarehouse"),
      "sfdatabase" -> props.getProperty("sfdatabase"),
      "sfschema" -> props.getProperty("sfschema")
    )

    val courseraPath = "D:\\coursera2"
    val courseraDF = spark.read.option("header", "true").csv(courseraPath)

    courseraDF.write
      .format("net.snowflake.spark.snowflake")
      .options(snowflake)
      .option("dbtable", "COURSES")
      .mode("overwrite")
      .save()

    // Reading DataFrame from Snowflake
    val df = spark.read
      .format("net.snowflake.spark.snowflake")
      .options(snowflake)
      .option("dbtable", "COURSES")
      .load()
    df.show(5)
  }
}
