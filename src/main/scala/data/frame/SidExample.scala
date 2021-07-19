package data.frame

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{aggregate, col, count, regexp_replace, sum}

object SidExample extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  val spark = SparkSession
    .builder()
    .appName("SparkSessionApp")
    .config("spark.master", "local[*]")
    .getOrCreate()
  val sc = spark.sparkContext

  //load the file
  val df = spark.read
    .option("header","true")
    .csv("src/main/resources/Airports2.csv")

  val schema = df.schema
  schema.printTreeString()
  df.printSchema()


  // Cuántos vuelos hubieron entre dos ciudades y cuántas viajaron entre ellas
  df.select("Origin_city","Destination_city","Passengers")
    .groupBy("Origin_city","Destination_city")
    .agg(count("Passengers").as("Nro Vuelos"), sum("Passengers").as("Total Pasajeros"))
    .show(10,false)


  //What are the structure?

  //¿Cuántas filas hay?


  // Cuáles son todas las regiones

  // Mostrar la cantidad de denuncias por región ordenadas en orden descendentes respecto a la cantidad
  // Borrar de los nombres de región la palabra ´REGPOL -´



  // Mostrar la cantidad de denuncias por region, agrupadas por sexo.


  // Mostrar la cantidad de denuncias por region, agrupadas por sexo.

  var getRegion = spark.udf.register("getRegion",(str:String) =>{
    val t = "^(\\w{1,})( {1,}- {1,})([\\w ]{1,})( {0,})$".r.findAllMatchIn(str)
    if(t.isEmpty) str else t.toList(0).group(3)
  })

  val dfPolicia = spark.read
    .option("header","true")
    .csv("src/main/resources/SIDPOL_2016_Violencia_familiar.csv")

  dfPolicia
    .selectExpr("SEXO","REGION")
    .groupBy("SEXO","REGION")
    .agg(count("SEXO").as("Cant Denuncias"))
    .withColumn("REGION",getRegion(col("REGION")))
    .where(col("SEXO")==="M" or col("SEXO")==="F" and col("Cant Denuncias")>10)
    .groupBy(col("REGION"))
    .pivot("SEXO")
    .count()
    .show(10,false)



}
