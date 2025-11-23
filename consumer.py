#!/usr/bin/env python3
import os
import time
import json
import signal
from typing import List, Tuple, Dict, Any
from datetime import datetime, timezone
from redis import Redis
import polars as pl

from app.storage import get_part_file_path, atomic_write_parquet
from app.logger import get_logger, setup_logging

# Inicializar logging
setup_logging()
logger = get_logger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
QUEUE_KEY = os.getenv("HEARTBEAT_QUEUE_KEY", "heartbeat:queue")
PROCESSING_KEY = os.getenv("HEARTBEAT_PROCESSING_KEY", "heartbeat:processing")

MAX_BATCH = int(os.getenv("MAX_BATCH", "400"))
MAX_BATCH_TIME = float(os.getenv("MAX_BATCH_TIME", "5.0"))  # segundos
BRPOP_TIMEOUT = int(os.getenv("BRPOP_TIMEOUT", "1"))       # segundos para BRPOPLPUSH

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
    try:
        s = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        return json.loads(s)
    except Exception as e:
        logger.warning(f"decode_item: invalid JSON or encoding error: {e}")
        return None

def epoch_ms_to_date_str(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")

def flush_batch_to_parts(batch: List[Tuple[bytes, Dict[str, Any]]]):
    """
    batch: list of tuples (raw_bytes, parsed_dict)
    -> agrupa por user_id + date y escribe 1 part file por grupo
    """
    if not batch:
        return

    groups = {}
    for raw, rec in batch:
        # espera que rec tenga 'timestamp_ms' o fallback a enqueued_at
        ts_ms = rec.get("timestamp_ms") or rec.get("enqueued_at")
        if ts_ms is None:
            # si no hay timestamp_ms, fallback a ahora
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
        else:
            try:
                date_str = epoch_ms_to_date_str(int(ts_ms))
            except Exception:
                date_str = datetime.utcnow().strftime("%Y-%m-%d")

        key = (rec.get("user_id", "unknown"), date_str)
        groups.setdefault(key, []).append(rec)

    for (user_id, date_str), records in groups.items():
        try:
            # normalizar schema explícito por si hay campos extra/incompletos
            # campos esperados: device_id,user_id,timestamp_ms,heart_rate,enqueued_at
            df = pl.DataFrame(records)
            # rename timestamp_ms -> timestamp_ms si existe; opcional convert
            part_path = get_part_file_path(user_id, date_str)
            atomic_write_parquet(df, part_path)
            logger.info(f"flush: wrote part {part_path} ({len(records)} rows) for user={user_id}")
        except Exception as e:
            logger.exception(f"flush_batch_to_parts error writing part for user={user_id} date={date_str}: {e}")
            # Propagar excepción para que el caller decida no remover LREM
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

            # flush conditions
            now = time.monotonic()
            if batch and (len(batch) >= MAX_BATCH or (now - last_flush) >= MAX_BATCH_TIME):
                raw_items = [b[0] for b in batch]
                records = [b[1] for b in batch]
                try:
                    flush_batch_to_parts(batch)
                    # Si fue exitoso, confirmamos borrando cada raw del processing list
                    for raw_item in raw_items:
                        try:
                            redis.lrem(PROCESSING_KEY, 1, raw_item)
                        except Exception:
                            logger.exception("Error removing processed item from processing list")
                except Exception:
                    # si falla el write, NO borrar de processing: quedará para reintento/watchdog
                    logger.exception("Batch flush failed; leaving items in processing for retry")
                batch.clear()
                last_flush = time.monotonic()
            # si no hubo raw (timeout), el loop vuelve a brpoplpush; éste diseño permite flush por tiempo
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
