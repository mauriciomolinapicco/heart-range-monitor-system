# Heart Rate Monitor System

Sistema para monitorear la frecuencia cardiaca de diferentes usuarios (cada usuario puede tener multiples dispositivos).

![System Design](sysdesign.jpg)

### Desafios
- Alta tasa de escrituras
- Optimizar queries

### Detalles tecnicos:
- Producer (FastAPI) recibe POST requests y hace `RPUSH` directamente a Redis
- Consumer lee de Redis usando `BRPOPLPUSH` (operacion atomica)
- Los datos se escriben en batches a archivos Parquet fragmentados (`part-*.parquet`)
- Un servicio compacter mergea los fragmentos en `compacted.parquet` periodicamente
- Prioridad de dispositivos solo se aplica cuando el timestamp es EXACTAMENTE el mismo
- Queries agregan datos por minuto y resuelven conflictos por prioridad de dispositivo

## Componentes 
### 1) Producer (FastAPI)
- Recibe POST /metrics/heart-rate
- Valida payload (30–220 bpm, timestamp ISO8601)
- Convierte timestamp → timestamp_ms (epoch ms)
- Encola en Redis vía RPUSH heartbeat:queue
- Responde Accepted inmediatamente

### 2) Redis Queue
Se encolan los jobs para ser procesador por el consumer. Se garantiza at-least-once delivery SIN perdida incluso si el servidor se cae

### 3) Consumer (daemon)
Se ejecuta cada cierta cantidad de tiempo o cuando se llega a cierto limite de elementos en la cola. Procesa las requests y almacena en archivos parquet. No hace append de archivos sino que en cada vuelta crea nuevos.
- Los archivos se guardan en un formato optimizado para las consultas mas frecuentes.
- Por eficiencia/normalizacion se almacena el timestamp en epoch ms

### 4) Compacter
Corre cada X minutos. El objetivo es reducir la cantidad de archivos. Se mantiene un archivo "compactado" por fecha por usuario y cuando se crean nuevos archivos al correr el compacter se unifica.

### 5) Query Engine
Endpoint like: GET /metrics/heart-rate?user_id=X&start=...&end=...&device_id=...
Entra al directorio data/YYYYMMDD/user_id y:
- lee compacted.parquet
- lee part-*.parquet recientes
- concatena y normaliza
- deduplica por mismo device y mismo timestamp
- aplica PRIORIDAD entre dispositivos
- promedios intervalos de 1 minuto 
- devuelve el json ordenado


### Flujo de registro de heartbeat:
1. Client → POST /metrics/heart-rate → Producer (FastAPI)
2. Producer → `RPUSH` a Redis lista `heartbeat:queue`
3. Consumer → `BRPOPLPUSH` mueve item a `heartbeat:processing`
4. Consumer → agrupa en batch y escribe a `part-*.parquet`
5. Consumer → `LREM` remueve de `processing` tras escritura exitosa
6. Compacter → mergea `part-*.parquet` en `compacted.parquet` periodicamente

### Como usar
Clona el repo y ejecuta:
```bash
docker-compose up
```

El servidor va a correr en http://localhost:8000

Endpoints:
- `POST /metrics/heart-rate` - registrar heartbeat
- `GET /metrics/heart-rate?user_id=X&start=Y&end=Z&device_id=W` - consultar datos
- `GET /health` - health check

### Mejoras futuras para prod
- usar SQS en lugar de Redis queue (incluir DLQ)
- escalar consumer/producer/ horizontalmente
- agregar metricas y monitoring
- use S3 for storage of parquet files
- implementar WATCHDOG


Author: Mauricio Molina
