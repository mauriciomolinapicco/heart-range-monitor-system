import json
import os
from typing import List, Dict, Any
from redis import Redis
from app.logger import get_logger

logger = get_logger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
BUFFER_KEY = os.getenv("HEARTBEAT_QUEUE_KEY", "heartbeat:queue")  # Key en Redis para la queue (mismo que consumer)

# cache de conexion redis (singleton)
_redis_client = None

def _get_redis() -> Redis:
    """Obtiene o crea la conexión a Redis (singleton por proceso)."""
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis.from_url(REDIS_URL, decode_responses=False)
    return _redis_client

def add_record(record: Dict[str, Any]) -> None:
    """Agrega un heartbeat al buffer en Redis (compartido entre procesos)."""
    try:
        redis_client = _get_redis()
        # Serializar el record a JSON y agregarlo a la lista en Redis
        record_json = json.dumps(record)
        redis_client.rpush(BUFFER_KEY, record_json)
        logger.debug(f"Record agregado al buffer Redis - user_id: {record.get('user_id')}")
    except Exception as e:
        logger.error(f"Error agregando record al buffer Redis: {e}", exc_info=True)
        raise

def get_and_clear_batch() -> List[Dict[str, Any]]:
    """Devuelve todos los registros del buffer Redis y lo limpia (operación atómica)."""
    try:
        redis_client = _get_redis()
        
        # Obtener todos los registros y limpiar el buffer de forma atómica
        # Usamos MULTI/EXEC para asegurar atomicidad
        pipe = redis_client.pipeline()
        pipe.lrange(BUFFER_KEY, 0, -1)  # Obtener todos los elementos
        pipe.delete(BUFFER_KEY)  # Limpiar el buffer
        results = pipe.execute()
        
        records_json = results[0]  # Lista de bytes
        if not records_json:
            return []
        
        # Deserializar los registros
        records = []
        for record_json in records_json:
            try:
                # Decodificar bytes a string y luego parsear JSON
                record_str = record_json.decode('utf-8') if isinstance(record_json, bytes) else record_json
                record = json.loads(record_str)
                records.append(record)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"Error deserializando record del buffer: {e}")
                continue
        
        logger.debug(f"Obtenidos {len(records)} registros del buffer Redis")
        return records
    except Exception as e:
        logger.error(f"Error obteniendo batch del buffer Redis: {e}", exc_info=True)
        return []

def get_size() -> int:
    """Obtiene el tamaño actual del buffer en Redis."""
    try:
        redis_client = _get_redis()
        size = redis_client.llen(BUFFER_KEY)
        return size
    except Exception as e:
        logger.error(f"Error obteniendo tamaño del buffer Redis: {e}", exc_info=True)
        return 0
