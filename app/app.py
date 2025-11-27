from fastapi import FastAPI, Depends, HTTPException, Request, Query
from fastapi.responses import JSONResponse
from redis import Redis
from app.models import Heartbeat
from app.storage import query_heart_rate_data
from app.logger import get_logger
from app.buffer import add_record
from app.util import datetime_to_epoch_ms
from app.config import REDIS_URL
from datetime import datetime, timezone
from typing import Optional
import os

logger = get_logger(__name__)

app = FastAPI()

@app.on_event("startup")
def startup_redis():
    logger.info("Iniciando aplicación...")
    redis = Redis.from_url(
        REDIS_URL,
        socket_connect_timeout=5,
        socket_keepalive=True, #orientado a conexion persistente
        health_check_interval=30, #revisar conexion
        retry_on_timeout=True 
    )
    try:
        redis.ping()
        logger.info("Conexión a Redis establecida correctamente")
    except Exception as e:
        logger.error(f"Error al conectar con Redis: {e}")
        raise RuntimeError("No se pudo conectar a Redis en startup") from e
    app.state.redis = redis
    logger.info("Aplicación iniciada correctamente")


@app.on_event("shutdown")
def shutdown_redis():
    logger.info("Cerrando aplicación...")
    try:
        app.state.redis.close()
        logger.info("Conexión a Redis cerrada")
    except Exception as e:
        logger.warning(f"Error al cerrar conexión Redis: {e}")


def get_redis(request: Request) -> Redis:
    """Obtiene la conexión de Redis del estado de la app."""
    redis = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(500, "Redis no inicializado")
    return redis


@app.get("/health")
async def health_check(redis: Redis = Depends(get_redis)):
    """
    Health check endpoint que verifica el estado del servicio.
    
    Returns:
        - status: "healthy" si todo está OK, "unhealthy" si hay problemas
        - checks: Detalle del estado de cada componente
        
    Status codes:
        - 200: Service is healthy
        - 503: Service is unhealthy (one or more checks failed)
    """
    checks = {
        "service": "healthy",
        "redis": "unknown",
        "storage": "unknown"
    }
    overall_status = "healthy"
    
    # Verificar Redis
    try:
        redis.ping()
        checks["redis"] = "healthy"
    except Exception as e:
        logger.error(f"Health check falló - Redis: {e}")
        checks["redis"] = f"unhealthy: {str(e)}"
        overall_status = "unhealthy"
    
    # Verificar storage (directorio data)
    try:
        data_dir = os.getenv("HEARTBEAT_DATA_DIR", "data")
        # Verificar que el directorio existe o se puede crear
        os.makedirs(data_dir, exist_ok=True)
        # Verificar que es escribible
        test_file = os.path.join(data_dir, ".health_check")
        try:
            with open(test_file, "w") as f:
                f.write("test")
            os.remove(test_file)
            checks["storage"] = "healthy"
        except Exception as e:
            checks["storage"] = f"unhealthy: {str(e)}"
            overall_status = "unhealthy"
    except Exception as e:
        logger.error(f"Health check falló - Storage: {e}")
        checks["storage"] = f"unhealthy: {str(e)}"
        overall_status = "unhealthy"
    
    status_code = 200 if overall_status == "healthy" else 503
    
    return JSONResponse(
        status_code=status_code,
        content={
            "status": overall_status,
            "checks": checks
        }
    )

#actual endpoints
@app.post("/metrics/heart-rate")
async def enqueue_heartbeat(payload: Heartbeat, redis: Redis = Depends(get_redis)):
    """
    Endpoint para recibir heartbeats.
    Agrega directamente a Redis usando RPUSH (sin RQ).
    """
    try:
        
        ts_dt = payload.timestamp  # tipo: datetime
        ts_ms = datetime_to_epoch_ms(ts_dt)

        # Crear el record
        record = {
            "device_id": payload.device_id,
            "user_id": payload.user_id,
            "timestamp_ms": ts_ms,
            "heart_rate": payload.heart_rate
        }
        
        # agregar a la "cola" de redis (buffer)
        add_record(record)
        
        logger.info(f"Heartbeat agregado a Redis - user_id: {payload.user_id}, device_id: {payload.device_id}")
        return {"status": "accepted"}
    except Exception as e:
        logger.error(f"Error al agregar heartbeat a Redis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error al procesar la solicitud")


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
        logger.warning(f"Invalid timestamp format - user_id: {user_id}, error: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timestamp format. Use ISO 8601 (ej: 2024-01-15T10:00:00Z). Error: {e}"
        )
    
    # validacion start < end
    if start_dt >= end_dt:
        logger.warning(f"Invalid date range - user_id: {user_id}, start: {start}, end: {end}")
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
