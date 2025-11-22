# app/tasks.py
"""
Logica de negocio para procesar los datos de heartbeat y almacenar en .parquet files
"""
from datetime import datetime, timezone
from typing import Dict, Any
from app.storage import get_file_path, append_to_parquet
from app.logger import get_logger
from app.buffer import add_record, get_size
import os

logger = get_logger(__name__)


def process_heartbeat(payload: Dict[str, Any]):
    # normalizo timestamp
    ts = payload.get("timestamp")
    if isinstance(ts, datetime):
        # Si es datetime, convertir a ISO format
        # Si tiene timezone, isoformat() ya incluye el timezone
        # Si no tiene timezone, asumir UTC
        if ts.tzinfo is None:
            # Sin timezone, agregar UTC y formatear con Z
            ts = ts.replace(microsecond=0).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        else:
            # Con timezone, convertir a UTC y formatear
            ts_utc = ts.replace(microsecond=0).astimezone(timezone.utc)
            ts = ts_utc.isoformat().replace("+00:00", "Z")
    elif isinstance(ts, str):
        # Si ya es string, asegurar que tenga formato correcto
        # Si termina en Z, está bien
        # Si tiene timezone pero no termina en Z, normalizar
        if not ts.endswith("Z") and "+" in ts:
            # Ya tiene timezone, convertir a formato con Z
            try:
                dt = datetime.fromisoformat(ts.replace("Z", ""))
                if dt.tzinfo:
                    dt_utc = dt.astimezone(timezone.utc)
                    ts = dt_utc.isoformat().replace("+00:00", "Z")
            except ValueError:
                pass  # Dejar como está si no se puede parsear

    record = {
        "device_id": payload["device_id"],
        "user_id": payload["user_id"],
        "timestamp": ts,
        "heart_rate": payload["heart_rate"]
    }

    # agrega al buffer en memoria
    add_record(record)
    logger.info(f"Heartbeat buffered - user_id: {payload['user_id']}, device_id: {payload['device_id']}, buffer size: {get_size()}")
    return {"status": "buffered", "user_id": payload["user_id"]}
