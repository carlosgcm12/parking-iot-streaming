# Plataforma de observabilidad IoT: aparcamiento urbano en Málaga

Pipeline de streaming que simula tres fuentes heterogéneas de datos de aparcamiento (parkings, zona azul y entradas/salidas de vehículos), las normaliza y enriquece con Spark Structured Streaming, las indexa en Elasticsearch y las visualiza en Kibana.

## Arquitectura

```
Fuente A (parkings)      ─┐
Fuente B (zona azul)     ─┼─> Kafka ─> Spark Structured Streaming (Scala) ─> Elasticsearch ─> Kibana
Fuente C (vehículos)     ─┘            normalizar · enriquecer
```

| Fuente | Topic Kafka | Qué emite |
|---|---|---|
| A | `parking-source-a` | Ocupación periódica de 10 parkings (plazas, tarifa, epoch Unix) |
| B | `parking-source-b` | Estado de ~400 sensores de zona azul, por evento (ISO 8601, batería) |
| C | `parking-source-c` | Entradas y salidas de vehículos con importe (fechas `dd/MM/yyyy`) |

## Estructura del repositorio

```
.
├── docker-compose.yml          Kafka (KRaft), Kafka UI, Elasticsearch, Kibana
├── elasticsearch/              Template, política ILM e índice inicial
├── generadores/                Generadores Python de las 3 fuentes
├── parking-normalizacion/      Job de Spark en Scala (sbt)
└── kibana/                     Dashboards, Data Views y mapas (.ndjson)
```

## Versiones

- Spark 3.4.2 (con 3.5.1 el conector de Elasticsearch falla con `NoSuchMethodError: RowEncoder$.apply`)
- Scala 2.13, sbt 1.9.9
- Conector `elasticsearch-spark-30_2.13` 8.14.3
- Elasticsearch y Kibana 8.14.3
- Python 3 con `confluent-kafka`

## Puesta en marcha

### 1. Levantar la infraestructura

```
docker compose up -d
docker compose ps
```

Servicios: Kafka `localhost:9092`, Kafka UI `localhost:8080`, Elasticsearch `localhost:9200`, Kibana `localhost:5601`.

### 2. Crear los topics

```
docker exec -it parking-kafka /opt/kafka/bin/kafka-topics.sh --create --topic parking-source-a --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it parking-kafka /opt/kafka/bin/kafka-topics.sh --create --topic parking-source-b --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it parking-kafka /opt/kafka/bin/kafka-topics.sh --create --topic parking-source-c --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 3. Configurar Elasticsearch

Orden obligatorio (PowerShell, desde la carpeta `elasticsearch/`): primero la política ILM, después el template y por último el índice inicial con el alias de rollover. Detalle en `elasticsearch/README_elasticsearch.md`.

```
curl.exe -X PUT "http://localhost:9200/_ilm/policy/parking-ilm-policy" -H "Content-Type: application/json" -d "@ilm_policy_parking.json"
curl.exe -X PUT "http://localhost:9200/_index_template/parking-template" -H "Content-Type: application/json" -d "@index_template_parking.json"
curl.exe -X PUT "http://localhost:9200/parking-000001" -H "Content-Type: application/json" -d "{\"aliases\": {\"parking\": {\"is_write_index\": true}}}"
```

### 4. Lanzar los generadores

Desde `generadores/`:

```
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python fuente_a_parkings.py
python fuente_b_zona_azul.py
python fuente_c_vehiculos.py
```

Cada generador en su propia terminal.

### 5. Lanzar Spark

Abrir `parking-normalizacion/` en IntelliJ como proyecto sbt y ejecutar `Parking.Main`. El job lee los tres topics, normaliza, enriquece y escribe en el alias `parking` de Elasticsearch (con checkpoint).

### 6. Importar los dashboards en Kibana

Kibana > Stack Management > Saved Objects > Import > seleccionar `kibana/saved_objects.ndjson`. Se importan los Data Views (`Parking-all`, `Parking - Street Sensor`, `Parking - anomalías`), los mapas y los dashboards `Main`, `Detalle Parkings` y `Detalle zona azul`.

## Procesamiento en Spark

- **Normalizar** (`Normalizar.scala`): traduce el formato de cada fuente a un esquema común (`EsquemaNormalizado`): timestamp ISO 8601, `location.point` como `geo_point`, identificadores y estado homogéneos.
- **Enriquecer** (`Enriquecer.scala`): calcula `ocupacion_pct` y una tarifa dinámica según la ocupación (<30 %: x0,85; 30-70 %: x1,00; 70-90 %: x1,15; >90 %: x1,30).
- **Validar** (`Validar.scala`): comprobaciones de coordenadas, plazas, fiabilidad del sensor y formato de fechas. La fiabilidad del sensor marca los eventos con `severity = warning`.

## Dashboards

- **Main**: visión global (estado de parkings, ocupación, tarifa dinámica, recaudación, zona azul, anomalías) con enlaces a los detalles.
- **Detalle Parkings**: vista de un parking concreto.
- **Detalle zona azul**: vista de un distrito concreto.

## Decisiones y limitaciones

- Los generadores B y C usan una aceleración temporal x20 para que la demo se vea en tiempo real.
- Seguridad de Elasticsearch desactivada (entorno de desarrollo).
- Retención de 24 horas: rollover cada hora y borrado a las 24 h (política ILM).
- De momento, `Validar` solo marca eventos como `warning`; no descarta eventos inválidos.
