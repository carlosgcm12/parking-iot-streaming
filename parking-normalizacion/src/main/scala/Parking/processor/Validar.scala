package Parking.processor

import scala.util.Try
import java.time.Instant
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object Validar {

  // VFalidar que las coordenadas etsán dentro del rango
  def validGeo(lat: Double, lon: Double): Boolean = {
    lat >= 36.65 && lat <= 36.75 && lon >= -4.55 && lon <= -4.35
  }

  // Comprobar que el número de plazas libres y ocupadas tiene sentido
  def plazasCoherentes(plazasLibres: Int, plazasTotales: Int): Boolean = {
    plazasLibres <= plazasTotales && plazasLibres >= 0 && plazasTotales >= 0
  }

  // Comprueba si el sensor es fiable o no
  def sensorFiable(estado: String, batteryPct: Double): Boolean = {
    estado != "UNKNOWN" && batteryPct >= 15
  }

  // Comprueba que el formato fecha de la fuente B es correcto
  def timestampIsoValido(timestamp: String): Boolean = {
    Try(Instant.parse(timestamp)).isSuccess
  }

  // Comprueba que el formato fecha de la fuente C
  def timestampEuropeoValido(fecha: String): Boolean = {
    val formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
    Try(LocalDateTime.parse(fecha, formatter)).isSuccess
  }

}
