from fastapi import FastAPI, Depends, HTTPException, Request, Query
from redis import Redis
from rq import Queue
from app.models import Heartbeat
from app.storage import query_heart_rate_data
from datetime import datetime
from typing import Optional
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

app = FastAPI()
@app.on_event("startup")
def startup_redis():
    redis = Redis.from_url(
        REDIS_URL,
        socket_connect_timeout=5,
        socket_keepalive=True, #orientado a conexion persistente
        health_check_interval=30, #revisar conexion
        retry_on_timeout=True 
    )
    try:
        redis.ping()
    except Exception as e:
        raise RuntimeError("No se pudo conectar a Redis en startup") from e
    app.state.redis = redis
    app.state.queue = Queue("high", connection=redis)

@app.on_event("shutdown")
def shutdown_redis():
    try:
        app.state.redis.close()
    except Exception:
        pass

def get_queue(request: Request):
    q = getattr(request.app.state, "queue", None)
    if q is None:
        raise HTTPException(500, "Redis no inicializado")
    return q

#actual endpoints
@app.post("/metrics/heart-rate")
async def enqueue_heartbeat(payload: Heartbeat, queue: Queue = Depends(get_queue)):
    # validacion de rango de heart rate (30-220) si no se cumple se devuelve error 422 por regla del modelo
    queue.enqueue("app.tasks.process_heartbeat", payload.dict(), job_timeout=600)
    return {"status": "accepted"}

# endpoint de consulta (parte 2)
@app.get("/metrics/heart-rate")
async def get_heart_rate(
    user_id: str = Query(..., description="ID del usuario"),
    start: str = Query(..., description="Fecha/hora de inicio en formato ISO 8601"),
    end: str = Query(..., description="Fecha/hora de fin en formato ISO 8601"),
    device_id: Optional[str] = Query(None, description="ID del dispositivo (opcional)")
):
    try:
        # parse timestamps ISO 8601
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timestamp format. Use ISO 8601 (ej: 2024-01-15T10:00:00Z). Error: {e}"
        )
    
    # validacion start < end
    if start_dt >= end_dt:
        raise HTTPException(
            status_code=400,
            detail="start date must be before end date"
        )
    
    try:
        data = query_heart_rate_data(user_id, start_dt, end_dt, device_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error querying data: {str(e)}"
        )
    
    return {
        "user_id": user_id,
        "data": data,
        "count": len(data)
    }
