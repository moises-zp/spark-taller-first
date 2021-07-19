package data.frame

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{SparkSession, functions}

object DfFromCassandra extends App {
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
    .config("spark.cassandra.connection.host", hostname)
    .config("spark.cassandra.connection.port", port)
//    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
    .getOrCreate()

  val dfUserCassandra =
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "user", "keyspace" -> principalKeySpace)).load()

  val dfContractCassandra =
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "contract", "keyspace" -> principalKeySpace))
      .load()

  /*
  Row(int, String, Map[], Set[])
  conjunto(vacio)
  setX.isEmpty
   */
  dfContractCassandra.printSchema()
  println(">>>>>>>>> explain0 ")
  dfContractCassandra.select("*")
    .where("contract_id != '0010AB'")
    .filter(r => !r.getList(8).isEmpty)
    .explain()

  dfContractCassandra.select("*")
    .where("contract_id != '0010AB'")
    .filter(r => !r.getList(8).isEmpty)
    .explain()

  println(">>>>>>>>> explain1 ")
  dfContractCassandra.select("*")
    .filter(r => !r.getList(8).isEmpty && !r.getString(0).equalsIgnoreCase("0010AB"))
    .explain()

  println(">>>>>>>>> explain2 ")
  dfContractCassandra.select("*")
    .where(col("contract_id") =!= "0010AB")
    .where(functions.size(col("users")) =!= 0)
    .explain()

  // Expresiones ejemplo:
  dfContractCassandra
    .selectExpr("concat(lower(contract_id),'ACTIVE') as CONTRACT_CODE")
    .explain()
  //.show(10,false)

  /* La siguiente línea no es válida debido a que ´select´ espera nombre de columnas
  dfContractCassandra
    .select("concat(lower(contract_id),'ACTIVE') as CONTRACT_CODE").show(10,false)
   */

  dfContractCassandra
    .select(concat(lower(col("contract_id")), lit("ACTIVE")).as("CONTRACT_CODE2"))
    .explain()

  // Seleccionar todo donde el usuario de seguridad tiene tipo de documento DNI
  dfContractCassandra
    .select("contract_id", "creation_date", "main_organization.name_organization", "status")
    .where(functions.size(col("organizations")) =!= 0)
    .show(5, false)

  // Cuántos usuarios tienen tipo de documento DNI, CE y PAS
  dfUserCassandra
    .select(col("user_document_type"))
    .groupBy("user_document_type")
    .agg(count(col("user_document_type")).as("count"))
    .orderBy(col("user_document_type").desc)
    .sort(col("user_document_type").desc)
    .explain

  dfContractCassandra
    .selectExpr(
      "upper(contract_id) as contract_id",
      "activation_date",
      s"""concat(
         |main_organization.document_number,
         |'-',
         |main_organization.document_type,
         |'-' ,
         |main_organization.name_organization)
         |as main_organization""".stripMargin,
      "size(organizations) as number_organizations",
      "size(security_users) as number_sec_users",
      "size(users) as number_users",
      "status"
    ).distinct()
    .explain

  dfContractCassandra
    .select(
      upper(col("contract_id")).as("contract_id"),
      expr("activation_date"),
      concat(
        expr("main_organization.document_number"),
        lit("-"),
        expr("main_organization.document_type"),
        lit('-') ,
        expr("main_organization.name_organization")
      ) as "main_organization",
      size(col("organizations")) as "number_organizations",
      size(col("security_users")) as "number_sec_users",
      size(col("users")) as "number_users",
      col("status")
    )
    .distinct()
    .explain

  dfContractCassandra
    .select("*")
    .drop("organizations","users","afiliation_date")
    .withColumn("contract_id",upper(col("contract_id")))
    .withColumn("activation_date",col("activation_date").cast(DateType))
    .withColumn("main_organization",
      concat(
        expr("main_organization.document_number"),
        lit("-"),
        expr("main_organization.document_type"),
        lit('-') ,
        expr("main_organization.name_organization")
      )
    )
    .withColumn("number_sec_users", size(col("security_users")))
    .distinct()
    .explain


  //UDF
  val prettyFormated2 = spark.udf.register("prettyFormated",
    (docType:String, docNumber:String, name:String) =>
      s"${name.capitalize} is identified by ${docType.toUpperCase} ${docNumber.toUpperCase}"
  )

  dfContractCassandra
    .selectExpr("contract_id",
      "prettyFormated(main_organization.document_type,main_organization.document_number,main_organization.name_organization) as main_organization",
      "explode(organizations) as organizations",
    )
    .withColumn("organizations",
      prettyFormated2(expr("organizations.document_type"),expr("organizations.document_number"),expr("organizations.name_organization"))
    )
    .show(false)


  val prettyFormated3 = functions.udf(
    (docType:String, docNumber:String, name:String) =>
      s"${name.capitalize} is identified by ${docType.toUpperCase} ${docNumber.toUpperCase}"
  )

  dfContractCassandra
    .selectExpr("contract_id",
      "main_organization",
      "explode(organizations) as organizations",
    )
    .withColumn("main_organization",
      prettyFormated3(expr("main_organization.document_type"),expr("main_organization.document_number"),expr("main_organization.name_organization")))
    .withColumn("organizations",
      prettyFormated3(expr("organizations.document_type"),expr("organizations.document_number"),expr("organizations.name_organization"))
    )
    .show(false)

  //Working with missing data
  // Elimina las filas en las que agún campo es nulo
  dfContractCassandra
    .select("*")
    .na
    .drop()
    .show(false)

  //Working with missing data
  // Elimina las filas en las que agún campo es nulo
  import com.mongodb.spark._

  MongoSpark.save(dfContractCassandra
    .select("*")
    .na
    .drop("security_users"::Nil)
  )

  case class Organization(
                           document_number: String,
                           document_type: String,
                           name_organization: String
                         )

  case class User(
                   document_number: String,
                   document_type: String
                 )

  case class Contract(
                       contract_id: String,
                       activation_date: String,
                       creation_date: String,
                       main_organization: Organization,
                       organizations: List[Organization],
                       security_users: List[User],
                       services_group: List[String],
                       users: List[User]
                     )

  import spark.implicits._
  dfContractCassandra
    .as[Contract]
    .filter(c => Option(c.users).exists(!_.isEmpty))
    .show(5,false)


  dfContractCassandra
    .agg(collect_set(col("main_organization.document_type")))
    .show(false)

  dfContractCassandra
    .select(collect_set(col("main_organization.document_type")))
    .show(false)

  dfContractCassandra
    .agg(count("main_organization"))
    .show(true)

  dfContractCassandra
    .select(count("main_organization"))
    .show(true)
}