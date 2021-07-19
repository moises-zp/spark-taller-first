package stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object StreamExample extends App{
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  System.setProperty("hadoop.home.dir", "E:\\hadoop-3.1.2")
  // parameter configuration sparkSesion
  val hostname = "127.0.0.1"
  val port = "9042"
  val tableExample = "contract"
  val principalKeySpace = "ntlc"


  val spark = SparkSession
    .builder()
    .appName("SparkSessionApp")
    .config("spark.master", "local[*]")
    .getOrCreate()
  val sc = spark.sparkContext

  val dfSidapol = spark
    .read
    .option("header","true")
    .csv("src/main/resources/SIDPOL_2016_Violencia_familiar.csv")

  val dfSchema = dfSidapol.schema
  dfSchema.printTreeString()

  val dfStream = spark
    .readStream
    .schema(dfSchema)
    .csv("src/main/resources/SIDPOL_2016_Violencia_familiar.csv")
    .groupBy(col("REGION"))
    .count()
    .select("*")

  dfStream
    .writeStream
    .queryName("activity_counts")
    .format("memory")
    .outputMode("complete")
    .start()


  println(dfStream.count())
}
