package chapter1

import org.apache.spark.sql.SparkSession
import org.apache.tika.parser.txt.CharsetDetector

object FirstExample extends App{

  //System.setProperty("hadoop.home.dir", "E:\\hadoop")
  val spark = SparkSession
    .builder
    .appName("Example-3_7")
    .master("local[*]")
    .getOrCreate()

  println(spark.sparkContext.appName)

  val random = new scala.util.Random

  val names = "Moisés"::"Ada"::"Luis"::"María"::"Pilar"::"Jua"::Nil
  val namesLength = names.length
  val minAge = 25
  val maxAge = 60

  def getPersons(nroOfPerson:Int) = for(i <- Range(0,nroOfPerson)) yield
    Person(names(random.nextInt(namesLength)), minAge + random.nextInt((maxAge - minAge + 1)))


  val teams = getPersons(100)
  val teamsRdd = spark.sparkContext.parallelize(teams)
  teamsRdd
    .filter(_.age > 40)
    .map(t => Person(t.name.toUpperCase,t.age))
   .map( _.toString)
    .map(str => {
      val detector = new CharsetDetector()
      detector.setText(str.getBytes())
      detector.detect()
      detector.getString(str.getBytes(), "utf-8")
    })
    .foreach(println(_))

}

case class Person(name:String, age:Int)