"""
Fuente A - Parkings publicos gestionados de Malaga.

Emisor PERIODICO: cada INTERVALO_SEGUNDOS, cada parking publica un unico
evento con el conteo agregado de plazas libres/totales, igual que el panel
luminoso de la entrada. No se modela el detalle de cada vehiculo (eso es
responsabilidad de la Fuente C).

Publica en el topic de Kafka: parking-source-a
"""

import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from parkings_catalogo import PARKINGS

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "parking-source-a"
INTERVALO_SEGUNDOS = 300  # en la practica real seria 300 (5 min); se deja corto para la demo

# Estado interno simulado: nº de coches actualmente dentro de cada parking.
# Este estado es el que tambien usara' (de forma independiente) la Fuente C.
# Inicialización del estado de todos los parkings con un valor aleatorio entre 0 y 60% de ocupación.
ocupacion_actual = {p["parking_id"]: random.randint(0, int(p["plazas_totales"] * 0.6)) for p in PARKINGS}

# Esta función simula el paso del tiempo y la variación de ocupación de cada parking, con un ligero sesgo hacia más entradas que salidas.
def simular_variacion_ocupacion(parking: dict) -> int:
    """Simula la entrada/salida neta de vehiculos desde la ultima emision."""
    actual = ocupacion_actual[parking["parking_id"]]
    variacion = random.randint(-15, 20)  # algo mas de entradas que salidas de media
    nuevo = actual + variacion
    nuevo = max(0, min(parking["plazas_totales"], nuevo))
    ocupacion_actual[parking["parking_id"]] = nuevo
    return nuevo


# Aquí se construye el diccionario que representa el evento a publicar en Kafka, con los campos requeridos.
def construir_evento(parking: dict) -> dict:
    ocupados = simular_variacion_ocupacion(parking)
    libres = parking["plazas_totales"] - ocupados
    return {
        "parking_id": parking["parking_id"],
        "nombre": parking["nombre"],
        "plazas_totales": parking["plazas_totales"],
        "plazas_libres": libres,
        "tarifa_hora": parking["tarifa_hora"],
        "lat": parking["lat"],
        "lon": parking["lon"],
        "ts_unix": int(datetime.now(timezone.utc).timestamp()),
    }


# Confirmción del envío de cada mensaje a Kafka, para debug y control de errores.
def delivery_report(err, msg):
    if err is not None:
        print(f"[Fuente A] ERROR al publicar: {err}")
    else:
        print(f"[Fuente A] -> {msg.topic()} | {msg.value().decode('utf-8')[:90]}...")


# Bucle principal: cada INTERVALO_SEGUNDOS, publica un evento por cada parking en el topic de Kafka.
def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    print(f"[Fuente A] Generador de parkings iniciado. Publicando en '{TOPIC}' "
          f"cada {INTERVALO_SEGUNDOS}s para {len(PARKINGS)} parkings.")

    try:
        while True:
            for parking in PARKINGS:
                evento = construir_evento(parking)
                producer.produce(
                    TOPIC,
                    key=parking["parking_id"],
                    value=json.dumps(evento),
                    callback=delivery_report,
                )
            producer.poll(1)  # da tiempo a procesar callbacks
            producer.flush()
            time.sleep(INTERVALO_SEGUNDOS)
    except KeyboardInterrupt:
        print("[Fuente A] Detenido manualmente.")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
