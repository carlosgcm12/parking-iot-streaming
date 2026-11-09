package Parking.processor

import Parking.model.EsquemaNormalizado

object Enriquecer {

  def calcularOcupacionPct(plazasTotales: Int, plazasLibres: Int): Double = {
    val ocupadas = plazasTotales - plazasLibres
    (ocupadas.toDouble / plazasTotales.toDouble) * 100
  }

  def calcularTarifaDinamica(tarifaBase: Double, ocupacionPct: Double): (Double, String) = {
    if (ocupacionPct < 30) {
      (tarifaBase * 0.85, "baja_ocupacion")
    } else if (ocupacionPct < 70) {
      (tarifaBase * 1.00, "ocupacion_normal")
    } else if (ocupacionPct < 90) {
      (tarifaBase * 1.15, "alta_ocupacion")
    } else {
      (tarifaBase * 1.30, "ocupacion_critica")
    }
  }

  def enriquecerEventoA(evento: EsquemaNormalizado): EsquemaNormalizado = {

    (evento.parking.plazas_totales, evento.parking.plazas_libres) match {
      case (Some(totales), Some(libres)) =>
        val ocupacionPct = calcularOcupacionPct(totales, libres)
        val tarifaBase = evento.parking.tarifa_hora_base_eur.getOrElse(0.0)
        val (tarifaDinamica, motivo) = calcularTarifaDinamica(tarifaBase, ocupacionPct)

        val parkingActualizado = evento.parking.copy(
          ocupacion_pct = Some(ocupacionPct),
          tarifa_hora_dinamica_eur = Some(tarifaDinamica),
          tarifa_aplicada = Some(motivo)
        )

        evento.copy(parking = parkingActualizado)

      case _ =>
        evento  // no es un evento de Fuente A (no tiene plazas), se devuelve igual
    }
  }

}
