"""
Fuente C - Entradas y salidas de vehiculos (parking y zona azul) de Malaga.

Emisor EVENT-DRIVEN con ESTADO: cada vehiculo simulado "entra" en una plaza
(de un parking de la Fuente A o de un sensor de zona azul de la Fuente B) y,
pasado un tiempo aleatorio, "sale". En el evento de salida se calcula el
importe:
  - Parking: duracion_horas x tarifa_hora del parking (tarifa BASE, sin
    pricing dinamico -> el ajuste dinamico se aplica despues en Spark).
  - Zona azul: 1 EUR/hora, pero solo sobre los minutos que caen dentro del
    horario de pago (9:00-20:00) y que no son domingo ni festivo de Malaga.

No depende de A ni de B en tiempo de ejecucion: usa los mismos catalogos
(parkings_catalogo.py, distritos_catalogo.py) como "fuente de verdad" de
referencias validas, igual que hacen A y B.

Publica en el topic de Kafka: parking-source-c
"""

import json
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone, date

from confluent_kafka import Producer

from parkings_catalogo import PARKINGS
from distritos_catalogo import DISTRITOS

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "parking-source-c"

FACTOR_ACELERACION = 20  # mismo factor que la Fuente B, para mantener proporciones

TARIFA_ZONA_AZUL_EUR_HORA = 1.00
HORARIO_PAGO_INICIO = 9   # 9:00
HORARIO_PAGO_FIN = 20     # 20:00

# Festivos de Malaga capital 2026 (nacionales + autonomicos + locales)
FESTIVOS_MALAGA_2026 = {
    date(2026, 1, 1), date(2026, 1, 6), date(2026, 2, 28),
    date(2026, 4, 2), date(2026, 4, 3), date(2026, 5, 1),
    date(2026, 8, 15), date(2026, 8, 19), date(2026, 9, 8),
    date(2026, 10, 12), date(2026, 11, 1), date(2026, 11, 2),
    date(2026, 12, 6), date(2026, 12, 7), date(2026, 12, 8), date(2026, 12, 25),
}

# Rangos de duracion de estancia (en MINUTOS REALES simulados; se acortan
# solo en el tiempo de espera del hilo, no en el valor que se reporta)
DURACION_ZONA_AZUL_MIN = (15, 180)       # 15 min - 3h (mayoria)
DURACION_ZONA_AZUL_LARGA_MIN = (240, 480)  # 4h-8h, ~8% de los casos
DURACION_PARKING_MIN = (30, 8 * 60)      # 30 min - 8h
DURACION_PARKING_LARGA_MIN = (600, 720)  # 10h-12h, ~5% de los casos

# Tiempo simulado entre nuevas "entradas" generadas por el propio proceso
NUEVA_ENTRADA_MIN_SEGUNDOS = 1.0
NUEVA_ENTRADA_MAX_SEGUNDOS = 4.0


def es_gratis(momento: datetime) -> bool:
    """True si ese instante cae fuera de horario de pago, en domingo o festivo."""
    if momento.weekday() == 6:  # domingo
        return True
    if momento.date() in FESTIVOS_MALAGA_2026:
        return True
    if not (HORARIO_PAGO_INICIO <= momento.hour < HORARIO_PAGO_FIN):
        return True
    return False


def minutos_facturables_zona_azul(entrada: datetime, salida: datetime) -> int:
    """Recorre el intervalo minuto a minuto (aprox, en pasos de 1 min) y cuenta
    solo los minutos que caen dentro de horario de pago y dia no festivo/domingo."""
    total_facturable = 0
    cursor = entrada
    while cursor < salida:
        siguiente = min(cursor + timedelta(minutes=1), salida)
        if not es_gratis(cursor):
            total_facturable += (siguiente - cursor).total_seconds() / 60
        cursor = siguiente
    return round(total_facturable)


def elegir_destino():
    """Elige aleatoriamente un parking (Fuente A) o un sensor de zona azul (Fuente B)
    como destino de la nueva entrada, usando los catalogos reales compartidos."""
    if random.random() < 0.5:
        parking = random.choice(PARKINGS)
        return {
            "tipo_ubicacion": "parking",
            "referencia": parking["parking_id"],
            "tarifa_hora": parking["tarifa_hora"],
            "lat": parking["lat"],
            "lon": parking["lon"],
        }
    else:
        distrito = random.choice(DISTRITOS)
        sensor_num = random.randint(1, distrito["n_sensores"])
        lat = distrito["lat"] + random.uniform(-0.004, 0.004)
        lon = distrito["lon"] + random.uniform(-0.004, 0.004)
        return {
            "tipo_ubicacion": "zona_azul",
            "referencia": f"STREET-SENS-{sensor_num:04d}",  # referencial, no exacto al de B
            "tarifa_hora": TARIFA_ZONA_AZUL_EUR_HORA,
            "lat": round(lat, 6),
            "lon": round(lon, 6),
        }


def construir_evento_entrada(vehiculo_id: str, destino: dict, entrada_dt: datetime) -> dict:
    return {
        "vehiculo": vehiculo_id,
        "tipo_ubicacion": destino["tipo_ubicacion"],
        "referencia": destino["referencia"],
        "accion": "entrada",
        "importe": None,
        "fecha": entrada_dt.strftime("%d/%m/%Y %H:%M:%S"),
        "geo": {"lat": destino["lat"], "lng": destino["lon"]},
    }


def construir_evento_salida(vehiculo_id: str, destino: dict, entrada_dt: datetime, salida_dt: datetime) -> dict:
    duracion_total_min = round((salida_dt - entrada_dt).total_seconds() / 60)

    if destino["tipo_ubicacion"] == "zona_azul":
        minutos_facturables = minutos_facturables_zona_azul(entrada_dt, salida_dt)
        importe = round((minutos_facturables / 60) * TARIFA_ZONA_AZUL_EUR_HORA, 2)
        tarifa_aplicada = "zona_azul_diurna" if minutos_facturables > 0 else "zona_azul_gratuita"
    else:
        minutos_facturables = duracion_total_min  # parking cobra 24h
        importe = round((duracion_total_min / 60) * destino["tarifa_hora"], 2)
        tarifa_aplicada = "parking_base"

    return {
        "vehiculo": vehiculo_id,
        "tipo_ubicacion": destino["tipo_ubicacion"],
        "referencia": destino["referencia"],
        "accion": "salida",
        "entrada_fecha": entrada_dt.strftime("%d/%m/%Y %H:%M:%S"),
        "salida_fecha": salida_dt.strftime("%d/%m/%Y %H:%M:%S"),
        "minutos_totales": duracion_total_min,
        "minutos_facturables": minutos_facturables,
        "importe": importe,
        "tarifa_aplicada": tarifa_aplicada,
        "geo": {"lat": destino["lat"], "lng": destino["lon"]},
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"[Fuente C] ERROR al publicar: {err}")


def elegir_duracion_minutos(tipo_ubicacion: str) -> int:
    if tipo_ubicacion == "zona_azul":
        if random.random() < 0.08:
            return random.randint(*DURACION_ZONA_AZUL_LARGA_MIN)
        return random.randint(*DURACION_ZONA_AZUL_MIN)
    else:
        if random.random() < 0.05:
            return random.randint(*DURACION_PARKING_LARGA_MIN)
        return random.randint(*DURACION_PARKING_MIN)


def hilo_estancia(producer: Producer, stop_event: threading.Event):
    """Simula una estancia completa de un vehiculo: entrada -> espera -> salida."""
    vehiculo_id = f"VEH-{random.randint(10000, 99999)}"
    destino = elegir_destino()
    entrada_dt = datetime.now(timezone.utc)

    evento_entrada = construir_evento_entrada(vehiculo_id, destino, entrada_dt)
    producer.produce(TOPIC, key=vehiculo_id, value=json.dumps(evento_entrada), callback=delivery_report)
    producer.poll(0)

    duracion_min_real = elegir_duracion_minutos(destino["tipo_ubicacion"])
    duracion_min_simulada = duracion_min_real  # se reporta la duracion "real" de dominio
    espera_segundos = (duracion_min_real * 60) / FACTOR_ACELERACION

    if stop_event.wait(espera_segundos):
        return  # el generador se esta parando, no emitimos la salida

    salida_dt = entrada_dt + timedelta(minutes=duracion_min_simulada)
    evento_salida = construir_evento_salida(vehiculo_id, destino, entrada_dt, salida_dt)
    producer.produce(TOPIC, key=vehiculo_id, value=json.dumps(evento_salida), callback=delivery_report)
    producer.poll(0)


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    stop_event = threading.Event()

    print(f"[Fuente C] Generador de entradas/salidas de vehiculos iniciado. "
          f"Publicando en '{TOPIC}'.")

    hilos_activos = []
    try:
        while True:
            t = threading.Thread(target=hilo_estancia, args=(producer, stop_event), daemon=True)
            t.start()
            hilos_activos.append(t)
            hilos_activos = [h for h in hilos_activos if h.is_alive()]

            producer.poll(0)
            espera = random.uniform(NUEVA_ENTRADA_MIN_SEGUNDOS, NUEVA_ENTRADA_MAX_SEGUNDOS)
            time.sleep(espera)
    except KeyboardInterrupt:
        print("[Fuente C] Deteniendo generador...")
        stop_event.set()
        for h in hilos_activos:
            h.join(timeout=1)
        producer.flush()
        print("[Fuente C] Detenido.")


if __name__ == "__main__":
    main()
