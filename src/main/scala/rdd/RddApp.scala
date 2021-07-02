package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object RddApp extends App{
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  System.setProperty("hadoop.home.dir", "E:\\hadoop")
  val spark = SparkSession
    .builder
    .appName("Example-3_7")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  //Load the data
  val rddPrimerVuelta = sc.textFile("E:\\data_taller\\Resultados_1ra_vuelta_Version_PCM.csv")
    .map(ResultadoActa.convertToResultadoActaVuelta2(_,Vuelta.PRIMERA))
    .cache()
  val rddSegundaVuelta = sc.textFile("E:\\data_taller\\Resultados_2da_vuelta_Version_PCM.csv")
    .map(ResultadoActa.convertToResultadoActaVuelta2(_,Vuelta.SEGUNDA))
    .cache()


  rddSegundaVuelta
    .take(20)
    .foreach(println(_))

  rddPrimerVuelta
    .take(20)
    .foreach(println(_))

  // Encontrar las mesas en las que un partido politico obtuvo cero votos en la segunda vuelta cuando en la primera obtuvo uno o mas votos
  val fuerzaPupular1 = encontrarMesaPrimeraVueltaNo0YSegundaVuelta0(rddPrimerVuelta,rddSegundaVuelta,_.partidoFuerzaPopular>0,_.partidoFuerzaPopular==0)
  val peruLibre1 = encontrarMesaPrimeraVueltaNo0YSegundaVuelta0(rddPrimerVuelta,rddSegundaVuelta,_.partidoPoliticoNacionalPeruLibre>0,_.partidoPoliticoNacionalPeruLibre==0)

  // Encontrar las mesas en las que un partido politico obtuvo cero votos en ambas vueltas
  val fuerzaPupular2 = encontrarMesaPrimeraVueltaNo0YSegundaVuelta0(rddPrimerVuelta,rddSegundaVuelta,_.partidoFuerzaPopular==0,_.partidoFuerzaPopular==0).cache
  val peruLibre2 = encontrarMesaPrimeraVueltaNo0YSegundaVuelta0(rddPrimerVuelta,rddSegundaVuelta,_.partidoPoliticoNacionalPeruLibre==0,_.partidoPoliticoNacionalPeruLibre==0).cache


  //Mostrar data:
  println(s"En ${fuerzaPupular1.count()} mesas Fuerza Popular tuvo uno o más votos en la primera vuelta mientras que en la seguna obtuvo 0 votos")
  println(s"En ${peruLibre1.count()} mesas Peru Libre tuvo uno o más votos en la primera vuelta mientras que en la seguna obtuvo 0 votos")

  println(s"En ${fuerzaPupular2.count()} mesas Fuerza Popular tuvo cero votos en la primera y segunda vuelta")
  println(s"En ${peruLibre2.count()} mesas Peru Libre tuvo cero votos en la primera y segunda vuelta")

  //Cuáles son las mesas en común en las que fuerza popular y peru libre no consiguieron votos en ambas vueltas que en la segunda vuelta
  val mesas0ComunAmbasVueltas = fuerzaPupular2
    .intersection(peruLibre2)
  println(s"En ${mesas0ComunAmbasVueltas.count()} mesas en las que Fuerza Popular y Peru Libre obtuvieron 0 votos en ambas vueltas")

  def encontrarMesaPrimeraVueltaNo0YSegundaVuelta0(
                                                    rddPrimerVuelta:RDD[ResultadoActa]
                                                    ,rddSegundaVuelta:RDD[ResultadoActa]
                                                    ,filtroPrimeraVuelta: ResultadoActa => Boolean
                                                    ,filtroSegundaVuelta: ResultadoActa => Boolean) = {

    val t = (rdd:RDD[ResultadoActa],filtro: ResultadoActa => Boolean ) => rdd.filter(filtro).map(_.mesaVotacion)

    val mesasVotos0SegundaVueltaK =  t(rddSegundaVuelta,filtroSegundaVuelta)
    val mesasVotosNo0PrimeraVueltaK = t(rddPrimerVuelta,filtroPrimeraVuelta)

    mesasVotos0SegundaVueltaK.intersection(mesasVotosNo0PrimeraVueltaK)
  }
}
