package data.frame

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import rdd.{ResultadoActa, Vuelta}

object RddToDF extends App{
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  //System.setProperty("hadoop.home.dir", "E:\\hadoop")
  val spark = SparkSession
    .builder
    .appName("Example-3_7")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext



  //Load the data
  val rddPrimerVuelta = sc.textFile("src/main/resources/Resultados_1ra_vuelta_Version_PCM.csv")
    .map(ResultadoActa.convertToResultadoActaVuelta2(_,Vuelta.PRIMERA))
    .cache()

  val rddSegundaVuelta = sc.textFile("src/main/resources/Resultados_2da_vuelta_Version_PCM.csv")
    .map(ResultadoActa.convertToResultadoActaVuelta2(_,Vuelta.SEGUNDA))
    .cache()

  import spark.implicits._

  val dfPrimeraVuelta = rddPrimerVuelta.toDF()
  dfPrimeraVuelta.printSchema()
  dfPrimeraVuelta.show(20,false)

  val usdPen:RDD[String] = sc.textFile("src/main/resources/Datos_historicos_USD_PEN.csv")
  val dfUsdPen = usdPen.map(_.split(",")).map({ case Array(f1,f2) => (f1,f2) }).toDF
  val dataStructure:StructType = StructType(Array(StructField("data",StringType, true)))
  //val dfUsdPen2 = spark.createDataFrame(usdPen,dataStructure)
  dfUsdPen.printSchema()


  import org.apache.spark.sql.types._

  val myDataType = new StructType()
    .add("name",new StructType()
      .add(StructField("firstName",StringType,true))
      .add("middleName",StringType)
      .add("lastName",StringType,true)
    )
    .add(StructField("dob",StringType,true))
    .add(StructField("gender",StringType,true))
    .add(StructField("salary",LongType,true))

  myDataType.printTreeString()

  val usdPenDataType = StructType(
    StructField("Fecha",DateType,false)::
      StructField("Ultimo",DoubleType,false)::
      StructField("Apertura",DoubleType,false)::
      StructField("Maximo",DoubleType,false)::
      StructField("Minimo",DoubleType,false)::
      StructField("% var",DoubleType,false)::
      Nil)
  usdPenDataType.printTreeString()
}
