package Parking.processor
import Parking.model.{DeviceInfo, EsquemaNormalizado, EventInfo, GeoPoint, LocationInfo, ParkingInfo}

import java.util.UUID
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.ZoneOffset

object Normalizar {

// Convertir segundos a un texto ISO 8601
  def unixATimestamp(tsUnix: Long): String = {
    val instante = Instant.ofEpochSecond(tsUnix)
    instante.toString
  }
  def fechaEuropeaATimestamp(fecha: String): String = {
    val formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
    val localDateTime = LocalDateTime.parse(fecha, formatter)
    localDateTime.toInstant(ZoneOffset.UTC).toString
  }


  def normalizarFuenteA(
                         parkingId: String,
                         nombre: String,
                         plazasTotales: Int,
                         plazasLibres: Int,
                         tarifaHora: Double,
                         lat: Double,
                         lon: Double,
                         tsUnix: Long
                       ): EsquemaNormalizado = {

    val eventInfo = EventInfo(
      id = UUID.randomUUID().toString,
      `type` = "parking_occupancy",
      source = "source_a",
      severity = "info"
    )
    // 2. Construye el DeviceInfo

    val deviceInfo = DeviceInfo(
      id = parkingId,
      `type` = "parking_garage",
      manufacturer = "smartparking"
    )
    // 3. Construye el GeoPoint y el LocationInfo
    val geoPoint = GeoPoint(
      lat = lat,
      lon = lon
    )

    val locationInfo = LocationInfo(
      point = geoPoint,
      site = parkingId,
      city = "Malaga",
      distrito = ""
    )
    // 4. Construye el ParkingInfo (recuerda: los campos que no aplican van con None,
    //    y los que sí aplican van envueltos en Some(...))
    val parkingInfo = ParkingInfo(
      plazas_totales = Some(plazasTotales),           // SÍ tiene valor en Fuente A
      plazas_libres = Some(plazasLibres),              // SÍ tiene valor en Fuente A
      ocupacion_pct = None,                            // se calculará luego en Enriquecer.scala
      tarifa_hora_base_eur = Some(tarifaHora),          // SÍ tiene valor en Fuente A
      tarifa_hora_dinamica_eur = None,                  // se calculará luego en Enriquecer.scala
      tarifa_aplicada = None,                           // esto es de la Fuente C, no de A
      battery_pct = None,                               // esto es de la Fuente B, no de A
      tipo_plaza = None,                                // esto es de la Fuente B, no de A
      tipo_ubicacion = None,                            // esto es de la Fuente C, no de A
      referencia = None,                                // esto es de la Fuente C, no de A
      accion = None,                                    // esto es de la Fuente C, no de A
      duracion_total_min = None,                        // esto es de la Fuente C, no de A
      duracion_facturable_min = None,                   // esto es de la Fuente C, no de A
      importe_eur = None                                // esto es de la Fuente C, no de A
    )
    // 5. Construye y devuelve el EsquemaNormalizado final, usando las piezas anteriores
    EsquemaNormalizado(
      `@timestamp` = unixATimestamp(tsUnix),
      event = eventInfo,
      device = deviceInfo,
      location = locationInfo,
      parking = parkingInfo,
      status = "operativo"
    )
  }

  def normalizarFuenteB(
                         sensor: String,
                         calle: String,
                         distrito: String,
                         estado: String,
                         lat: Double,
                         lon: Double,
                         timestamp: String,
                         batteryPct: Double,
                         tipoPlaza: String
                       ): EsquemaNormalizado = {

    val severity = if (Validar.sensorFiable(estado, batteryPct)) "info" else "warning"

    val eventInfo = EventInfo(
      id = UUID.randomUUID().toString,
      `type` = "sensor_status",
      source = "source_b",
      severity = severity
    )

    val deviceInfo = DeviceInfo(
      id = sensor,
      `type` = "street_sensor",
      manufacturer = "ser_malaga"
    )

    val geoPoint = GeoPoint(
      lat = lat,
      lon = lon
    )

    val locationInfo = LocationInfo(
      point = geoPoint,
      site = calle,
      city = "Malaga",
      distrito = distrito
    )

    val parkingInfo = ParkingInfo(
      plazas_totales = None,
      plazas_libres = None,
      ocupacion_pct = None,
      tarifa_hora_base_eur = None,
      tarifa_hora_dinamica_eur = None,
      tarifa_aplicada = None,
      battery_pct = Some(batteryPct),
      tipo_plaza = Some(tipoPlaza),
      tipo_ubicacion = None,
      referencia = None,
      accion = None,
      duracion_total_min = None,
      duracion_facturable_min = None,
      importe_eur = None
    )

    EsquemaNormalizado(
      `@timestamp` = timestamp,
      event = eventInfo,
      device = deviceInfo,
      location = locationInfo,
      parking = parkingInfo,
      status = estado.toLowerCase
    )
  }

  def normalizarFuenteCEntrada(
                              vehiculo: String,
                              tipo_ubicacion: String,
                              referencia: String,
                              fecha: String,
                              lat: Double,
                              lon: Double

                              ): EsquemaNormalizado = {

    val eventInfo = EventInfo(
      id = UUID.randomUUID().toString,
      `type` = "vehicle_event",
      source = "source_c",
      severity = "info"
    )

    val deviceInfo = DeviceInfo(
      id = vehiculo,
      `type` = "vehicle",
      manufacturer = ""
    )

    val geoPoint = GeoPoint(
      lat = lat,
      lon = lon
    )

    val locationInfo = LocationInfo(
      point = geoPoint,
      site = referencia,
      city = "Malaga",
      distrito = ""
    )

    val parkingInfo = ParkingInfo(
      plazas_totales = None,
      plazas_libres = None,
      ocupacion_pct = None,
      tarifa_hora_base_eur = None,
      tarifa_hora_dinamica_eur = None,
      tarifa_aplicada = None,
      battery_pct = None,
      tipo_plaza = None,
      tipo_ubicacion = Some(tipo_ubicacion),
      referencia = Some(referencia),
      accion = Some("entrada"),
      duracion_total_min = None,
      duracion_facturable_min = None,
      importe_eur = None
    )

    EsquemaNormalizado(
      `@timestamp` = fechaEuropeaATimestamp(fecha),
      event = eventInfo,
      device = deviceInfo,
      location = locationInfo,
      parking = parkingInfo,
      status = "entry"
    )

  }

  def normalizarFuenteCSalida(
                                vehiculo: String,
                                tipo_ubicacion: String,
                                referencia: String,
                                entrada_fecha: String,
                                salida_fecha: String,
                                minutos_totales: Int,
                                minutos_facturables: Int,
                                importe: Double,
                                tarifa_aplicada: String,
                                lat: Double,
                                lon: Double

                              ): EsquemaNormalizado = {
    val eventInfo = EventInfo(
      id = UUID.randomUUID().toString,
      `type` = "vehicle_event",
      source = "source_c",
      severity = "info"
    )

    val deviceInfo = DeviceInfo(
      id = vehiculo,
      `type` = "vehicle",
      manufacturer = ""
    )

    val geoPoint = GeoPoint(
      lat = lat,
      lon = lon
    )

    val locationInfo = LocationInfo(
      point = geoPoint,
      site = referencia,
      city = "Malaga",
      distrito = ""
    )

    val parkingInfo = ParkingInfo(
      plazas_totales = None,
      plazas_libres = None,
      ocupacion_pct = None,
      tarifa_hora_base_eur = None,
      tarifa_hora_dinamica_eur = None,
      tarifa_aplicada = Some(tarifa_aplicada),
      battery_pct = None,
      tipo_plaza = None,
      tipo_ubicacion = Some(tipo_ubicacion),
      referencia = Some(referencia),
      accion = Some("salida"),
      duracion_total_min = Some(minutos_totales),
      duracion_facturable_min = Some(minutos_facturables),
      importe_eur = Some(importe)
    )

    EsquemaNormalizado(
      `@timestamp` = fechaEuropeaATimestamp(salida_fecha),
      event = eventInfo,
      device = deviceInfo,
      location = locationInfo,
      parking = parkingInfo,
      status = "exit"
    )
  }


}
