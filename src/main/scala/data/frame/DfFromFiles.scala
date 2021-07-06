package data.frame

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.Date

object DfFromFiles extends App{

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession
    .builder
    .appName("Example-3_7")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  val usdPenDataType = StructType(
    StructField("Fecha",DateType,false)::
      StructField("Ultimo",DoubleType,false)::
      StructField("Apertura",DoubleType,false)::
      StructField("Maximo",DoubleType,false)::
      StructField("Minimo",DoubleType,false)::
      StructField("% var.",DoubleType,false)::
      Nil
  )

  fromRddFile()
  fromRddRow(usdPenDataType)
  dfFromFileInferScema()
  dfFromFileWithoutInferScema(usdPenDataType)

  def dfFromFileWithoutInferScema(usdPenDataType:StructType) = {
    val usdPenDFWithoutSchemaInfer = spark
      .read
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","false")
      .option("dateFormat", "dd.MM.yyyy")
      .option("decimalFormat", "#.####")
      .schema(usdPenDataType)
      .csv("src/main/resources/Datos_historicos_USD_PEN-2010-2021.csv")

    println("################ <<<<< dfFromFileWithoutInferScema, file Datos_historicos_USD_PEN-2010-2021>>>>> ################")
    usdPenDFWithoutSchemaInfer.printSchema()
    usdPenDFWithoutSchemaInfer.show(5,false)
    println("################ <<<<<------>>>>> ################\n\n")
  }


  def dfFromFileInferScema() = {
    val usdPenDF = spark
      .read
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .option("dateFormat", "dd.MM.yyyy")
      .option("decimalFormat", "#.####")
      .csv("src/main/resources/Datos_historicos_USD_PEN-2010-2021.csv")

    println("################ <<<<< dfFromFileInferScema, file Datos_historicos_USD_PEN-2010-2021>>>>> ################")
    usdPenDF.printSchema()
    usdPenDF.show(10,false)
    println("################ <<<<<------>>>>> ################\n\n")
  }

  def fromRddFile() = {
    import spark.implicits._
    val rdd = sc.textFile("src/main/resources/Datos_historicos_USD_PEN.csv")
    val dfFromRdd = rdd.toDF()

    println("################ <<<<< fromRddFile, file Datos_historicos_USD_PEN>>>>> ################")
    dfFromRdd.printSchema()
    dfFromRdd.show(5,false)
    println("################ <<<<<------>>>>> ################\n\n")
  }

  def fromRddRow(usdPenDataType: StructType) = {
    val rddUsdPen1:RDD[Row] = sc
      .textFile("src/main/resources/Datos_historicos_USD_PEN.csv")
      .map(_.replace("\",\"","\";\"").split(";"))
      .map({
        case Array(date,ultimo,apertura,maximo,minimo,varia) =>{
          Row(
            Date.valueOf(date.replace("\"","").split('.').reverse.mkString("-")),
            ultimo.replace("\"","").replace(",",".").toDouble,
            apertura.replace("\"","").replace(",",".").toDouble,
            maximo.replace("\"","").replace(",",".").toDouble,
            minimo.replace("\"","").replace(",",".").toDouble,
            varia.replace("\"","").replace(",",".").replace("%","").toDouble
          )} })

    val t = spark.createDataFrame(rddUsdPen1,usdPenDataType)

    println("################ <<<<< fromRddRow(usdPenDataType: StructType), file Datos_historicos_USD_PEN>>>>> ################")
    t.printSchema()
    t.show(5,false)
    t.selectExpr("Fecha","max(Ultimo) - 0.5").show(10)

    println("################ <<<<<------>>>>> ################\n\n")
  }
}

/**
Tipo de datos -> Row(Column), Column

 Spark funciones
 */

