# app/tasks.py
"""
Logica de negocio para procesar los datos de heartbeat y almacenar en .parquet files
"""
from datetime import datetime
from typing import Dict, Any
from app.storage import get_file_path, append_to_parquet
from app.logger import get_logger

logger = get_logger(__name__)


def process_heartbeat(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tarea RQ: recibe un dict (payload) y agrega el registro al archivo Parquet
    correspondiente al usuario y fecha.
    
    Estructura: data/YYYY-MM-DD/user_{user_id}.parquet
    """
    # Extraer campos necesarios
    user_id = payload.get("user_id")
    if not user_id:
        logger.error("process_heartbeat: user_id es requerido en el payload")
        raise ValueError("user_id es requerido en el payload")
    
    # si no tiene timestamp usa el actual
    timestamp_str = payload.get("timestamp")
    if timestamp_str:
        try:
            if isinstance(timestamp_str, str):
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            else:
                dt = timestamp_str
            date_str = dt.strftime("%Y-%m-%d")
        except Exception as e:
            logger.warning(f"process_heartbeat: Error parsing timestamp, usando fecha actual - error: {e}")
            #en caso de excepcion usa fecha actual
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
    else:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
    
    file_path = get_file_path(user_id, date_str)
    
    record = {
        "device_id": payload.get("device_id", ""),
        "user_id": user_id,
        "timestamp": timestamp_str or datetime.utcnow().isoformat() + "Z",
        "heart_rate": payload.get("heart_rate")
    }
    
    try:
        append_to_parquet(record, file_path)
        logger.info(f"Heartbeat procesado - user_id: {user_id}, device_id: {record['device_id']}, date: {date_str}, file: {file_path}")
    except Exception as e:
        logger.error(f"Error al escribir heartbeat - user_id: {user_id}, file: {file_path}, error: {e}", exc_info=True)
        raise
    
    # return metadata
    return {
        "written_path": file_path,
        "user_id": user_id,
        "date": date_str,
        "status": "appended"
    }

