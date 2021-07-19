package example

import data.frame.DfFromCassandra.{hostname, port, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_json

object EjemplosQuery extends App{
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
    .config("spark.cassandra.connection.host", hostname)
    .config("spark.cassandra.connection.port", port)
    .getOrCreate()

  val dfUserCassandra =
  spark.read.json("src/main/resources/data-prueba/users.json")

  val dfContratosCassandra =
    spark.read.json("src/main/resources/data-prueba/contratos.json")

  val dfCardCassandra =
    spark.read.json("src/main/resources/data-prueba/tarjetas.json")

  dfUserCassandra.printSchema()
  dfContratosCassandra.printSchema()
  dfCardCassandra.printSchema()


  import spark.implicits._
  /*dfUserCassandra.map(_.json)
    .show(100, false)*/

  dfCardCassandra
    .map(_.json)
    .show(100, false)

  val json = spark.read.json("src/main/resources/data-prueba/users.json")
  json.show(5,false)

  //Cuáles son los contratos en los que no hay usuarios de seguridad

  /*
  Mostra un reporte en los que se muestren los contratos, las empresas y los usuarios que pueden ingresa
  La salida deberá tener el siguiente formto:
  contrato:String
  nombre_empresa[tipo documento, nroDocumento]:String
  nombre_usuario
  Ejm:
  contrato | empresa | usuario
  102030  | Luz del Sur[RUC, 2010405612] | Moisés Zapata [DNI, 59013522]
  102030  | Luz del Sur[RUC, 2010405612] | Edson Sánchez [CE, 2010]
   */

  /*
  Cuáles son los usuarios que no tienen una tarjeta asignada pero que sin embargo están configurados en un contratp
  Mostrar la siguiente salida:
  contrato <- String
  nombre_usuario <- String

   */

}

