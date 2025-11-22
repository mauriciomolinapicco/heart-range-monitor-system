# app/batcher.py
import time
import threading
import polars as pl
from datetime import datetime
from collections import defaultdict
from app.buffer import get_and_clear_batch
from app.storage import get_file_path, atomic_write_parquet
from app.logger import get_logger

FLUSH_INTERVAL = 5  # segundos

logger = get_logger(__name__)

# Singleton: variables globales para controlar el thread del batcher
_batcher_thread = None
_batcher_thread_lock = threading.Lock()
_batcher_running = False

def batch_loop():
    """Loop que corre en un thread separado."""
    global _batcher_running
    logger.info(f"Batcher loop iniciado - intervalo: {FLUSH_INTERVAL}s")
    _batcher_running = True
    
    try:
        while _batcher_running:
            time.sleep(FLUSH_INTERVAL)
            
            batch = get_and_clear_batch()
            
            if not batch:
                continue

            logger.info(f"Batcher: procesando {len(batch)} registros del buffer")

            # agrupar por usuario+fecha
            groups = defaultdict(list)
            for rec in batch:
                ts = rec["timestamp"]
                try:
                    # Manejar diferentes formatos de timestamp
                    if isinstance(ts, str):
                        # Limpiar el formato: remover Z duplicado si existe
                        if ts.endswith("Z") and "+00:00" in ts:
                            # Formato incorrecto como "2025-11-22T05:09:45+00:00Z"
                            ts = ts.replace("+00:00Z", "+00:00").replace("Z", "")
                        elif ts.endswith("Z"):
                            # Formato correcto con Z: "2025-11-22T05:09:45Z"
                            ts = ts[:-1] + "+00:00"
                        elif "+" not in ts and "-" in ts:
                            # Formato sin timezone: agregar UTC
                            ts = ts + "+00:00"
                        # Si ya tiene formato correcto con timezone, usar directamente
                        dt = datetime.fromisoformat(ts)
                    elif isinstance(ts, datetime):
                        dt = ts
                    else:
                        logger.warning(f"Timestamp inválido en record: {ts} (tipo: {type(ts)})")
                        continue
                    
                    date_str = dt.strftime("%Y-%m-%d")
                    key = (rec["user_id"], date_str)
                    groups[key].append(rec)
                except (ValueError, AttributeError) as e:
                    logger.error(f"Error parseando timestamp '{ts}': {e}")
                    continue

            # escribir parquet por cada grupo
            for (user_id, date_str), records in groups.items():
                try:
                    import os
                    new_df = pl.DataFrame(records)
                    file_path = get_file_path(user_id, date_str)
                    
                    # Si el archivo existe, leerlo y concatenar con los nuevos registros
                    if os.path.exists(file_path):
                        existing_df = pl.read_parquet(file_path)
                        combined_df = pl.concat([existing_df, new_df])
                    else:
                        combined_df = new_df
                    
                    # Escribir el DataFrame combinado
                    atomic_write_parquet(combined_df, file_path)
                    logger.info(f"Batcher: escrito {len(records)} registros - user_id: {user_id}, date: {date_str}, path: {file_path}")
                except Exception as e:
                    logger.error(f"Batcher: error escribiendo registros - user_id: {user_id}, date: {date_str}, error: {e}", exc_info=True)
    finally:
        _batcher_running = False
        logger.info("Batcher loop finalizado")

def start_batcher_thread():
    """Inicia el thread del batcher como singleton (solo una instancia)."""
    global _batcher_thread, _batcher_running
    
    with _batcher_thread_lock:
        # Si ya hay un thread corriendo, no crear otro
        if _batcher_thread is not None and _batcher_thread.is_alive():
            logger.debug("Batcher thread ya está corriendo, no se inicia otro")
            return
        
        # Si el flag está activo pero el thread no está vivo, resetear
        if _batcher_running and (_batcher_thread is None or not _batcher_thread.is_alive()):
            logger.warning("Batcher thread marcado como corriendo pero no está vivo, reseteando...")
            _batcher_running = False
        
        # Crear y iniciar nuevo thread
        _batcher_thread = threading.Thread(target=batch_loop, daemon=True, name="BatcherThread")
        _batcher_thread.start()
        logger.info("Batcher thread iniciado (singleton)")

def stop_batcher_thread():
    """Detiene el thread del batcher."""
    global _batcher_thread, _batcher_running
    
    with _batcher_thread_lock:
        _batcher_running = False
        if _batcher_thread is not None and _batcher_thread.is_alive():
            logger.info("Deteniendo batcher thread...")
            # El thread se detendrá en la siguiente iteración del loop
