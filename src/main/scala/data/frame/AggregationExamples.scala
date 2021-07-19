package data.frame

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_set, max, min}

object AggregationExamples extends App{

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  System.setProperty("hadoop.home.dir", "E:\\hadoop")
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

  val dfAirlines = spark.read
    .option("header","true")
    .csv("src/main/resources/airlines.csv")
    .toDF

  val dfAirports = spark.read
    .option("header","true")
    .csv("src/main/resources/airports.csv")
    .toDF

  val dfFlights = spark.read
    .option("header","true")
    .csv("src/main/resources/flights.csv")
    .toDF

  dfAirlines.createOrReplaceTempView("dfAirlines")

  dfAirlines.printSchema()
  dfAirports.printSchema()
  dfFlights.printSchema()

  // Calcular los vuelos cancelados agrupados por origen y destino de
  dfFlights
    .select("CANCELLED")
    .agg(collect_set("CANCELLED")) // collect_list
    .show(false)

  val df = dfFlights
    .where("CANCELLED == 1")
    .groupBy("ORIGIN_AIRPORT","DESTINATION_AIRPORT")
    .count()
    .orderBy(col("ORIGIN_AIRPORT").desc_nulls_last,col("DESTINATION_AIRPORT").desc_nulls_last)

    df.createOrReplaceTempView("vuelos_cancelados")

  val vuelosCancelados = spark.sql("select * from vuelos_cancelados")
  vuelosCancelados.show(false)

  //JoinExpressions
  val originJE = vuelosCancelados.col("ORIGIN_AIRPORT") === dfAirports.col("IATA_CODE")// ob.===(p) <> ob === p
  val destinationJE = vuelosCancelados.col("DESTINATION_AIRPORT") === dfAirports.col("IATA_CODE")


  vuelosCancelados
    .join(dfAirports, originJE)
    .select(
      dfAirports.col("AIRPORT").as("ORIGIN"),
      vuelosCancelados.col("DESTINATION_AIRPORT").as("DESTINATION_AIRPORT"),
      vuelosCancelados.col("count").as("count")
    )

    .show(false)

}
