"""
Módulo para manejo del buffer Redis.
Proporciona funciones para agregar registros a la cola de Redis.
"""
import json
from typing import Dict, Any
from redis import Redis
from app.logger import get_logger
from app.config import REDIS_URL, BUFFER_KEY

logger = get_logger(__name__)

# Cache de conexión Redis (singleton por proceso)
_redis_client = None


def _get_redis() -> Redis:
    """
    Obtiene o crea la conexión a Redis (singleton por proceso).
    
    Returns:
        Cliente Redis configurado
    """
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis.from_url(REDIS_URL, decode_responses=False)
    return _redis_client


def add_record(record: Dict[str, Any]) -> None:
    """
    Agrega un heartbeat al buffer en Redis usando RPUSH.
    
    El registro se serializa a JSON y se agrega a la cola compartida
    entre el producer y el consumer.
    
    Args:
        record: Diccionario con los datos del heartbeat
        
    Raises:
        Exception: Si falla la conexión o escritura en Redis
    """
    try:
        redis_client = _get_redis()
        record_json = json.dumps(record)
        redis_client.rpush(BUFFER_KEY, record_json)
        logger.debug(f"Record agregado al buffer Redis - user_id: {record.get('user_id')}")
    except Exception as e:
        logger.error(f"Error agregando record al buffer Redis: {e}", exc_info=True)
        raise

