package rdd

import rdd.Vuelta.{PRIMERA, Vuelta}

object Vuelta extends Enumeration {
  type Vuelta = Value
  val PRIMERA,SEGUNDA = Value
}

case class ResultadoActa(
                          ubigeo: String,
                          departamento: String,
                          provincia: String,
                          distrito: String,
                          tipoEleccion: String,
                          mesaVotacion: String,
                          descripcionEstadoActa: String,
                          tipoObservacion: Option[String],
                          nroVcas: Int,
                          nroElectoresHabilitados: Int,
                          nroVotoBlanco: Int,
                          nroVotoNulo: Int,
                          nroVotoViciado: Int,
                          partidoNacionalistaPeruano: Int,
                          partidoFrenteAmplio: Int,
                          partidoMorado: Int,
                          partidoPeruPatriaSegura: Int,
                          partidoVictoriaNacional: Int,
                          partidoAccionPopular: Int,
                          partidoAvanzaPaisPartidoIntegracionSocial: Int,
                          partidoPodemosPeru: Int,
                          partidoJuntosPorElPeru: Int,
                          partidoPopularCristianoPpc: Int,
                          partidoFuerzaPopular: Int, // columna 13 en vuelta 2
                          partidoUnionPorElPeru: Int,
                          partidoRenovacionPopular: Int,
                          partidoRenacimientoUnidoNacional: Int,
                          partidoDemocraticoSomosPeru:Int,
                          partidoPoliticoNacionalPeruLibre: Int,// columna 12 en vuelta 2
                          partidoDemocraciaDirecta: Int,
                          partidoAlianzaParaElProgreso: Int
                        )

object ResultadoActa {
  private def getValue (arreglo:Array[String], indice:Int, deltaPartidos:Int=0) = if(arreglo(indice + deltaPartidos).replace("\"","").trim.isEmpty) 0 else arreglo(indice + deltaPartidos).replace("\"","").toInt


  private def getNroVotos(arreglo:Array[String],vuelta:Vuelta,indice:Int) = vuelta match {
    case PRIMERA => getValue(arreglo,indice)
    case _ => indice match{
      case 20 => getValue(arreglo,11)
      case 25 => getValue(arreglo,10)
      case _ => 0
    }
  }

  val getStringValue = (arreglo:Array[String], indice:Int) => arreglo(indice).replace("\"","")

  def convertToResultadoActaVuelta2(line: String, vuelta:Vuelta) = {
    val values = (line + ";").replace(";;",";\"\";").split(";")
    // En la primera vuelta hay 18 partidos y en la segunda hay 2
    val deltaPartidos = if(vuelta==PRIMERA) 16 else 0

    try{
      ResultadoActa(
      getStringValue(values,0) //ubigeo
      ,getStringValue(values,1) //departamento
      ,getStringValue(values,2) //provincia
      ,getStringValue(values,3) //distrito
      ,getStringValue(values,4) //tipoEleccion
      ,getStringValue(values,5) //mesaVotacion
      ,getStringValue(values,6) //descripcionEstadoActa
      ,if(values(7).replace("\"","").trim.isEmpty) None else Some(values(0)) //tipoObservacion
      ,getValue(values,8) //nroVcas
      ,getValue(values,9) //nroElectoresHabilitados
      ,getValue(values,12,deltaPartidos ) //nroVotoBlanco
      ,getValue(values,13,deltaPartidos ) //nroVotoNulo
      ,if(values.size<(15+deltaPartidos) || values(14 + deltaPartidos).replace("\"","").trim.isEmpty) 0 else values(14 + deltaPartidos).replace("\"","").toInt //nroVotoViciado
      ,getNroVotos(values,vuelta,10)
        ,getNroVotos(values,vuelta,11)
        ,getNroVotos(values,vuelta,12)
        ,getNroVotos(values,vuelta,13)
        ,getNroVotos(values,vuelta,14)
        ,getNroVotos(values,vuelta,15)
        ,getNroVotos(values,vuelta,16)
        ,getNroVotos(values,vuelta,17)
        ,getNroVotos(values,vuelta,18)
        ,getNroVotos(values,vuelta,19)
        ,getNroVotos(values,vuelta,20)
        ,getNroVotos(values,vuelta,21)
        ,getNroVotos(values,vuelta,22)
        ,getNroVotos(values,vuelta,23)
        ,getNroVotos(values,vuelta,24)
        ,getNroVotos(values,vuelta,25)
        ,getNroVotos(values,vuelta,26)
        ,getNroVotos(values,vuelta,27)
      )
    } catch{
      case e:Throwable => {
        println(line)
        throw e
      }
    }
  }


}

