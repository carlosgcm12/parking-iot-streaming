"""
Fuente B - Sensores individuales en calle (zona azul / SER) de Malaga.

Emisor EVENT-DRIVEN: cada uno de los 400 sensores simulados cambia de
estado (libre <-> ocupado) de forma independiente y publica un evento
solo cuando ese cambio ocurre. Ademas, cada sensor pierde bateria muy
lentamente con cada evento, lo que permite ver con el tiempo sensores
por debajo del umbral critico (15%).

Publica en el topic de Kafka: parking-source-b
"""

import json
import random
import threading
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from distritos_catalogo import DISTRITOS

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "parking-source-b"

# En la realidad cada plaza cambia de media cada 3-10 minutos. Se acelera
# con FACTOR_ACELERACION para que la demo se vea "viva" sin esperar minutos
# reales, manteniendo la MISMA proporcion relativa respecto a la Fuente A.
FACTOR_ACELERACION = 20
CAMBIO_MIN_SEGUNDOS = (3 * 60) / FACTOR_ACELERACION   # ~9s
CAMBIO_MAX_SEGUNDOS = (10 * 60) / FACTOR_ACELERACION  # ~30s

BATERIA_INICIAL_MIN = 40
BATERIA_INICIAL_MAX = 100
BATERIA_CRITICA_PCT = 15    # por debajo de este umbral, el sensor empieza a fallar mas
PROB_UNKNOWN_BASE = 0.02        # probabilidad de UNKNOWN con bateria normal
PROB_UNKNOWN_CRITICA = 0.35     # probabilidad de UNKNOWN con bateria critica (< 15%)
TIPOS_PLAZA = ["SER", "SER", "SER", "PMR", "CARGA_DESCARGA"]  # SER es lo mas comun


def construir_sensores():
    """Crea los 400 sensores repartidos por distrito, cada uno con su propio estado interno."""
    sensores = []
    contador = 1
    for distrito in DISTRITOS:
        for _ in range(distrito["n_sensores"]):
            sensor_id = f"STREET-SENS-{contador:04d}"
            lat = distrito["lat"] + random.uniform(-0.004, 0.004)
            lon = distrito["lon"] + random.uniform(-0.004, 0.004)
            sensores.append({
                "sensor_id": sensor_id,
                "distrito": distrito["nombre"],
                "calle": f"{distrito['calle_base']} {random.randint(1, 120)}",
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "tipo_plaza": random.choice(TIPOS_PLAZA),
                "estado": random.choice(["OCCUPIED", "FREE"]),
                "battery_pct": random.randint(BATERIA_INICIAL_MIN, BATERIA_INICIAL_MAX),
            })
            contador += 1
    return sensores


def construir_evento(sensor: dict) -> dict:
    # la probabilidad de fallo de lectura (UNKNOWN) depende de la bateria:
    # con bateria normal es rara (PROB_UNKNOWN_BASE); por debajo del umbral
    # critico, se dispara hasta PROB_UNKNOWN_CRITICA, simulando que un sensor con poca energia falla mas a menudo antes de dejar de emitir.
    if sensor["battery_pct"] < BATERIA_CRITICA_PCT:
        prob_unknown = PROB_UNKNOWN_CRITICA
    else:
        prob_unknown = PROB_UNKNOWN_BASE
 
    if random.random() < prob_unknown:
        sensor["estado"] = "UNKNOWN"
    else:
        sensor["estado"] = "FREE" if sensor["estado"] == "OCCUPIED" else "OCCUPIED"
 
    # desgaste de bateria: baja muy poco a poco, con algo de variabilidad
    sensor["battery_pct"] = max(0, sensor["battery_pct"] - random.uniform(0.05, 0.3))
 
    return {
        "sensor": sensor["sensor_id"],
        "calle": sensor["calle"],
        "distrito": sensor["distrito"],
        "estado": sensor["estado"],
        "position": {"lat": sensor["lat"], "lon": sensor["lon"]},
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "battery_pct": round(sensor["battery_pct"], 1),
        "tipo_plaza": sensor["tipo_plaza"],
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"[Fuente B] ERROR al publicar: {err}")


def hilo_sensor(sensor: dict, producer: Producer, stop_event: threading.Event):
    """Cada sensor corre en su propio hilo, emitiendo eventos a intervalos aleatorios."""
    while not stop_event.is_set():
        espera = random.uniform(CAMBIO_MIN_SEGUNDOS, CAMBIO_MAX_SEGUNDOS)
        if stop_event.wait(espera):
            break
        evento = construir_evento(sensor)
        producer.produce(
            TOPIC,
            key=sensor["sensor_id"],
            value=json.dumps(evento),
            callback=delivery_report,
        )
        producer.poll(0)


def main():
    sensores = construir_sensores()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    stop_event = threading.Event()

    print(f"[Fuente B] Generador de sensores de zona azul iniciado. "
          f"{len(sensores)} sensores en {len(DISTRITOS)} distritos. "
          f"Publicando en '{TOPIC}'.")

    hilos = []
    for sensor in sensores:
        t = threading.Thread(target=hilo_sensor, args=(sensor, producer, stop_event), daemon=True)
        t.start()
        hilos.append(t)

    try:
        # hilo principal: hace flush periodico y muestra un contador simple
        contador_flush = 0
        while True:
            time.sleep(2)
            producer.flush(0)
            contador_flush += 1
            if contador_flush % 5 == 0:
                print(f"[Fuente B] ... generando eventos ({len(sensores)} sensores activos)")
    except KeyboardInterrupt:
        print("[Fuente B] Deteniendo generador...")
        stop_event.set()
        for t in hilos:
            t.join(timeout=1)
        producer.flush()
        print("[Fuente B] Detenido.")


if __name__ == "__main__":
    main()
