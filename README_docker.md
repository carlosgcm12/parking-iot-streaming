# Entorno Docker — Reto IoT Parking Málaga

## Servicios incluidos

| Servicio | Puerto local | Para qué |
|---|---|---|
| Kafka (KRaft) | `localhost:9092` | Broker donde publican los 3 generadores Python y del que lee Spark |
| Kafka UI | `localhost:8080` | Ver topics, mensajes y particiones en el navegador |
| Elasticsearch | `localhost:9200` | Indexación de eventos normalizados |
| Kibana | `localhost:5601` | Data Views, mapas y dashboard |

Spark **no** está en el compose: se ejecuta desde IntelliJ (Scala) conectándose a
`localhost:9092` (Kafka) y `localhost:9200` (Elasticsearch), exactamente igual que
si fuera un cliente externo más.

## Arrancar el entorno

```bash
docker compose up -d
docker compose ps        # comprobar que los 4 servicios están "healthy"/"running"
```

## Crear los 3 topics (uno por fuente)

Con Kafka ya arrancado:

```bash
docker exec -it parking-kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic parking-source-a --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec -it parking-kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic parking-source-b --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec -it parking-kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic parking-source-c --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

Verificar que se crearon:

```bash
docker exec -it parking-kafka /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:9092
```

(También puedes verlos directamente en Kafka UI: http://localhost:8080)

## Verificar Elasticsearch

```bash
curl http://localhost:9200
curl http://localhost:9200/_cluster/health?pretty
```

## Acceder a Kibana

Abrir en el navegador: http://localhost:5601

## Apagar el entorno

```bash
docker compose down          # conserva los volúmenes (datos de ES y Kafka)
docker compose down -v       # borra también los volúmenes (empezar de cero)
```

## Notas

- `xpack.security.enabled=false` en Elasticsearch simplifica la práctica (sin usuario/contraseña).
  Es razonable documentarlo en la memoria técnica como decisión de entorno de desarrollo,
  no de producción.
- Los tres topics tienen 3 particiones para poder practicar paralelismo en el consumo desde
  Spark, aunque para el volumen de esta práctica no es estrictamente necesario.
- Si tu máquina va justa de recursos, puedes bajar `ES_JAVA_OPTS` a `-Xms512m -Xmx512m`.
