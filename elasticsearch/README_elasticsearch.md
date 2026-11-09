# Configuración de Elasticsearch — índice `parking-*`

Orden obligatorio: primero la política ILM (el template la referencia),
luego el index template, y por último el índice inicial con su alias de
rollover. Si lo haces en otro orden, Elasticsearch rechazará el template
porque no encontrará la política ILM.

Todos los comandos usan `curl.exe` (PowerShell) contra `http://localhost:9200`.

## 1. Crear la política ILM (rollover cada 1h, borrado a las 24h)

```powershell
curl.exe -X PUT "http://localhost:9200/_ilm/policy/parking-ilm-policy" `
  -H "Content-Type: application/json" `
  -d "@ilm_policy_parking.json"
```

Verificar que se creó:
```powershell
curl.exe "http://localhost:9200/_ilm/policy/parking-ilm-policy?pretty"
```

## 2. Crear el index template

```powershell
curl.exe -X PUT "http://localhost:9200/_index_template/parking-template" `
  -H "Content-Type: application/json" `
  -d "@index_template_parking.json"
```

Verificar que se creó:
```powershell
curl.exe "http://localhost:9200/_index_template/parking-template?pretty"
```

## 3. Crear el índice inicial con el alias de rollover

El index template usa `index.lifecycle.rollover_alias: parking`, así que
necesitamos crear el primer índice manualmente con ese alias marcado como
`is_write_index`. A partir de aquí, ILM se encarga de las siguientes
generaciones automáticamente.

```powershell
curl.exe -X PUT "http://localhost:9200/parking-000001" `
  -H "Content-Type: application/json" `
  -d "{\"aliases\": {\"parking\": {\"is_write_index\": true}}}"
```

Verificar:
```powershell
curl.exe "http://localhost:9200/_alias/parking?pretty"
```

Deberías ver `parking-000001` como `is_write_index: true`.

## 4. (Más adelante) Cómo escribirá Spark

Spark, al usar el conector de Elasticsearch, escribirá contra el **alias**
`parking` (no contra `parking-000001` directamente), para que el rollover
automático sea transparente:

```
es.resource = "parking/_doc"
es.nodes = "localhost"
es.port = "9200"
```

## 5. Comprobar que el mapping se aplicó correctamente

```powershell
curl.exe "http://localhost:9200/parking-000001/_mapping?pretty"
```

Deberías ver `location.point` como `geo_point`, `@timestamp` como `date`,
y el resto de campos con los tipos definidos en `index_template_parking.json`.

## Notas sobre la política ILM elegida

- **Fase hot**: rollover cada 1 hora (o antes si el índice supera 5GB,
  poco probable en esta práctica). Esto es lo que decide "cuándo se crea
  una nueva generación de índice".
- **Fase delete**: borrado a las 24 horas de la creación del índice, que
  es el requisito mínimo del reto (retención máxima de 24h).
- Para pruebas más rápidas durante el desarrollo (ver el rollover/delete
  en acción sin esperar 24h reales), se puede bajar temporalmente
  `min_age` de la fase delete a `10m` y `max_age` de rollover a `5m`,
  y volver a subirlo a los valores finales antes de la entrega —
  el propio enunciado del reto permite esto explícitamente.
