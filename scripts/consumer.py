#!/usr/bin/env python3
import os
import sys
import time
import json
import signal
from typing import List, Tuple, Dict, Any
from datetime import datetime
from redis import Redis
import polars as pl

# Agregar el directorio raíz al PYTHONPATH para que pueda encontrar el módulo 'app'
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from app.storage import get_part_file_path, atomic_write_parquet
from app.logger import get_logger, setup_logging
from app.util import epoch_ms_to_date_str
from app.config import (
    REDIS_URL,
    QUEUE_KEY,
    PROCESSING_KEY,
    MAX_BATCH,
    MAX_BATCH_TIME,
    BRPOP_TIMEOUT,
)

# Inicializar logging
setup_logging()
logger = get_logger(__name__)

# Redis client (binary-safe)
redis = Redis.from_url(REDIS_URL, decode_responses=False)

# Control graceful shutdown
_should_stop = False
def _signal_handler(sig, frame):
    global _should_stop
    logger.info(f"Signal {sig} received, shutting down consumer gracefully...")
    _should_stop = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

def decode_item(raw: bytes) -> Dict[str, Any]:
    """
    Decodifica un item de bytes a diccionario JSON.
    
    Args:
        raw: Bytes del item de Redis
        
    Returns:
        Diccionario parseado o None si hay error
    """
    try:
        s = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        return json.loads(s)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        logger.warning(f"decode_item: invalid JSON or encoding error: {e}")
        return None
    except Exception as e:
        logger.warning(f"decode_item: unexpected error: {e}")
        return None

def flush_batch_to_parts(batch: List[Tuple[bytes, Dict[str, Any]]]) -> None:
    """
    Agrupa registros por user_id y fecha, y escribe archivos part-*.parquet.
    
    Args:
        batch: Lista de tuplas (raw_bytes, parsed_dict) con los registros a procesar
        
    Raises:
        Exception: Si falla la escritura de algún archivo parquet
    """
    if not batch:
        return

    # Agrupar registros por (user_id, date_str)
    groups = {}
    for raw, rec in batch:
        # Extraer timestamp_ms o usar fallback
        ts_ms = rec.get("timestamp_ms") or rec.get("enqueued_at")
        if ts_ms is None:
            # Si no hay timestamp, usar fecha actual como fallback
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
            logger.warning(f"Record sin timestamp_ms, usando fecha actual: {rec.get('user_id')}")
        else:
            try:
                date_str = epoch_ms_to_date_str(int(ts_ms))
            except (ValueError, TypeError) as e:
                logger.warning(f"Error parseando timestamp_ms {ts_ms}: {e}, usando fecha actual")
                date_str = datetime.utcnow().strftime("%Y-%m-%d")

        user_id = rec.get("user_id", "unknown")
        key = (user_id, date_str)
        groups.setdefault(key, []).append(rec)

    # Escribir un archivo part-*.parquet por cada grupo (user_id, date)
    for (user_id, date_str), records in groups.items():
        try:
            # Crear DataFrame con los registros del grupo
            # Polars maneja automáticamente campos faltantes o extra
            df = pl.DataFrame(records)
            part_path = get_part_file_path(user_id, date_str)
            atomic_write_parquet(df, part_path)
            logger.info(f"flush: wrote part {part_path} ({len(records)} rows) for user={user_id} date={date_str}")
        except Exception as e:
            logger.exception(f"flush_batch_to_parts error writing part for user={user_id} date={date_str}: {e}")
            # Propagar excepción para que el caller no remueva items de processing
            raise

def consumer_loop():
    batch: List[Tuple[bytes, Dict[str, Any]]] = []
    last_flush = time.monotonic()

    logger.info("Consumer daemon started, listening to queue...")
    while not _should_stop:
        try:
            # BRPOPLPUSH: atomically move item from queue -> processing
            raw = redis.brpoplpush(QUEUE_KEY, PROCESSING_KEY, timeout=BRPOP_TIMEOUT)
            if raw:
                parsed = decode_item(raw)
                if parsed is None:
                    # item corrupto: remover 1 unidad de processing para no bloquear
                    try:
                        redis.lrem(PROCESSING_KEY, 1, raw)
                        logger.warning("Removed corrupt item from processing list")
                    except Exception:
                        logger.exception("Failed to remove corrupt item from processing")
                else:
                    batch.append((raw, parsed))

            # Verificar condiciones de flush: tamaño del batch o tiempo transcurrido
            now = time.monotonic()
            should_flush = batch and (
                len(batch) >= MAX_BATCH or 
                (now - last_flush) >= MAX_BATCH_TIME
            )
            
            if should_flush:
                raw_items = [b[0] for b in batch]
                try:
                    flush_batch_to_parts(batch)
                    # Si fue exitoso, confirmar borrando cada item del processing list
                    for raw_item in raw_items:
                        try:
                            redis.lrem(PROCESSING_KEY, 1, raw_item)
                        except Exception:
                            logger.exception("Error removing processed item from processing list")
                except Exception:
                    # Si falla el write, NO borrar de processing: quedará para reintento
                    logger.exception("Batch flush failed; leaving items in processing for retry")
                finally:
                    batch.clear()
                    last_flush = time.monotonic()
        except Exception as e:
            logger.exception(f"consumer_loop unexpected error: {e}")
            time.sleep(1)  # backoff

    # shutdown: intentar flush final (no confirmar LREM si falla)
    if batch:
        try:
            logger.info("Shutdown: flushing remaining batch before exit")
            flush_batch_to_parts(batch)
            for raw_item, _ in batch:
                try:
                    redis.lrem(PROCESSING_KEY, 1, raw_item)
                except Exception:
                    logger.exception("Error removing processed item from processing list during shutdown")
        except Exception:
            logger.exception("Final flush during shutdown failed; items remain in processing queue")

if __name__ == "__main__":
    consumer_loop()
