package data.frame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DfFromCassandra extends App{

  System.setProperty("hadoop.home.dir", "E:\\hadoop")
  // parameter configuration sparkSesion
  val hostname =  "127.0.0.1"
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
      .options(Map( "table" -> "user", "keyspace" -> principalKeySpace )).load()

  val dfContractCassandra =
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "contract", "keyspace" -> principalKeySpace )).load()




  dfUserCassandra.printSchema()
  dfUserCassandra.select("*")
    .filter(r => !r.getList(8).isEmpty)
    //.where(col("users"))
    .show(5,false)
}
