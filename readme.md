# Heart Rate Monitor System

We are building a system to monitor the heart rate of different users (each user can have multiple devices). 

### Challenges
- High rate of writes 
- Optimize queries

### Techical details:
- redis session and queue stored in app.state -> fastapi global container for shared objects
- used external stored queue (redis queue) so that it persists the data even if the server fails. ENSURES that all requests that received 200 http response will not be lost. Used RQ library for python
- only uses priority management when timestamp is EXACTLY the same

Register a heartbeat
1. Client → POST /metrics/heart-rate → Producer (FastAPI)
2. Producer → queue.enqueue("app.tasks.process_heartbeat", ...) → Redis
3. Consumer → detects job in queue
4. Consumer → excecutes app.tasks.process_heartbeat()
5. Consumer → append_to_parquet() → writes file

### How to use
Clone this repo and just execute in terminal
```bash
docker-compose up
```

You will have the endpoints available on http://localhost:8000/metrics/heart-rate


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
