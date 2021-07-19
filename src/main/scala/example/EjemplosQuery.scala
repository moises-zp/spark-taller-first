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
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "user", "keyspace" -> principalKeySpace)).load()

  val dfContratosCassandra =
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "contract", "keyspace" -> principalKeySpace)).load()

  val dfCardCassandra =
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "card", "keyspace" -> principalKeySpace)).load()


  import spark.implicits._
  /*dfUserCassandra.map(_.json)
    .show(100, false)*/

  dfCardCassandra
    .map(_.json)
    .show(100, false)

  val json = spark.read.json("src/main/resources/users.json")
  json.show(5,false)

}
