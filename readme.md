# Heart Rate Monitor System

We are building a system to monitor the heart rate of different users (each user can have multiple devices). 

### Challenges
- High rate of writes 
- Optimize queries

### Techical details:
- redis session and queue stored in app.state -> fastapi global container for shared objects
- used external stored queue (redis queue) so that it persists the data even if the server fails. ENSURES that all requests that received 200 http response will not be lost
- only uses priority management when timestamp is EXACTLY the same

1. Cliente → POST /metrics/heart-rate → Producer (FastAPI)
2. Producer → queue.enqueue("app.tasks.process_heartbeat", ...) → Redis
3. Consumer → detecta job en cola "high"
4. Consumer → ejecuta app.tasks.process_heartbeat()
5. Consumer → append_to_parquet() → escribe archivo

Author: Mauricio Molina

