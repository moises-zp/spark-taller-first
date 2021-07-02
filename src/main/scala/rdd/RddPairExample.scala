package rdd

import chapter1.Person
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RddPairExample extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  //System.setProperty("hadoop.home.dir", "E:\\hadoop")
  val spark = SparkSession
    .builder
    .appName("Example-3_7")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  // Create data
  val random = new scala.util.Random

  val names = "Moises"::"Ada"::"Luis"::"Maria"::"Pilar"::"Juan"::Nil
  val namesLength = names.length
  val minAge = 25
  val maxAge = 60

  def getPersons(nroOfPerson:Int) = for(i <- Range(0,nroOfPerson)) yield
    Person(names(random.nextInt(namesLength)), minAge + random.nextInt((maxAge - minAge + 1)))

  var persons = getPersons(1000)
  val personsRdd = sc.parallelize(persons)

  personsRdd
    .groupBy(_.age)
    .map((f: (Int,Iterable[Person]))=> (f._1,f._2.size))
    .sortBy(f=> f._1)
    .foreach(f => println(s"El numero de personas con edad ${f._1} es de ${f._2}"))


  val namesRdd = sc.parallelize(names)
  namesRdd
    .map(p => (p.length,p))
    .groupBy(p => p._1,2)
    .collect()
    .foreach(println(_))

}
