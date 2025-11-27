# app/storage.py
"""
funciones para el manejo de archivos .parquet
operaciones de escritura - locking - gestion de directorios
"""
import os
import fcntl
import tempfile
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta, timezone
import polars as pl
from app.logger import get_logger
from uuid import uuid4

logger = get_logger(__name__)

from app.config import DATA_DIR, DEVICE_PRIORITY, CANONICAL_COLS


def ensure_dir(path: str) -> None:
    """crea el directorio si no existe."""
    os.makedirs(path, exist_ok=True)


def get_part_file_path(user_id: str, date_str: str) -> str:
    """
    Genera la ruta completa para un archivo part-*.parquet.
    
    Args:
        user_id: ID del usuario
        date_str: Fecha en formato YYYY-MM-DD
        
    Returns:
        Ruta completa del archivo part-*.parquet
    """
    dest_dir = os.path.join(DATA_DIR, date_str, f"user-{user_id}")
    ensure_dir(dest_dir)
    filename = f"part-{uuid4().hex}.parquet"
    return os.path.join(dest_dir, filename)


def atomic_write_parquet(df: pl.DataFrame, dest_path: str) -> None:
    """
    Escribe un DataFrame a parquet de forma atómica usando archivo temporal.
    
    Usa un archivo temporal y luego lo reemplaza para garantizar atomicidad.
    Si falla la escritura, limpia el archivo temporal.
    
    Args:
        df: DataFrame de Polars a escribir
        dest_path: Ruta destino del archivo parquet
        
    Raises:
        Exception: Si falla la escritura del parquet
    """
    tmp_fd, tmp_path = tempfile.mkstemp(
        suffix=".parquet",
        dir=os.path.dirname(dest_path)
    )
    os.close(tmp_fd)
    try:
        # compresion snappy para mejor rendimiento
        df.write_parquet(tmp_path, compression="snappy")
        os.replace(tmp_path, dest_path)
    except Exception:
        # Limpiar archivo temporal en caso de error
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        raise


def append_to_parquet(new_record: Dict[str, Any], file_path: str, max_retries: int = 3) -> None:
    """Append a record to parquet file with file locking and retries."""
    new_df = pl.DataFrame([new_record])
    lock_file_path = file_path + ".lock"
    
    for attempt in range(max_retries):
        lock_file = None
        try:
            # Crear directorio si no existe
            ensure_dir(os.path.dirname(file_path))
            
            # Adquirir lock exclusivo usando un archivo de lock
            # El archivo se crea temporalmente solo para el lock
            lock_file = open(lock_file_path, "w")
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            
            try:
                # Leer archivo existente si existe
                if os.path.exists(file_path):
                    existing_df = pl.read_parquet(file_path)
                    # Concatenar con el nuevo registro
                    combined_df = pl.concat([existing_df, new_df])
                else:
                    combined_df = new_df
                
                atomic_write_parquet(combined_df, file_path)
                return 
            finally:
                # liberar lock y cerrar archivo
                if lock_file:
                    try:
                        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    except Exception:
                        pass
                    lock_file.close()
                if os.path.exists(lock_file_path):
                    try:
                        os.remove(lock_file_path)
                    except Exception:
                        pass  
        except Exception as e:
            if lock_file and not lock_file.closed:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    lock_file.close()
                except Exception:
                    pass
            if os.path.exists(lock_file_path):
                try:
                    os.remove(lock_file_path)
                except Exception:
                    pass
            
            if attempt == max_retries - 1:
                logger.error(f"append_to_parquet fallo despues de {max_retries} intentos - file: {file_path}, error: {e}", exc_info=True)
                raise RuntimeError(f"error al escribir en {file_path} despues de {max_retries} intentos: {e}") from e
            logger.warning(f"append_to_parquet reintento {attempt + 1}/{max_retries} - file: {file_path}, error: {e}")
            time.sleep(0.01 * (2 ** attempt))


def read_user_data(user_id: str, start: datetime, end: datetime) -> pl.DataFrame:
    """
    lee compacted.parquet + part-*.parquet para un user/date range.
    normaliza a esquema canonico: ["timestamp_ms","heart_rate","device_id","user_id"]
    convierte timestamp_ms -> timestamp (Datetime UTC).
    retorna DataFrame listo para agregaciones.
    """
    dataframes = []

    current = start.date()
    end_date = end.date()

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        folder = os.path.join(DATA_DIR, date_str, f"user-{user_id}")

        if os.path.isdir(folder):
            # compacted
            compacted_path = os.path.join(folder, "compacted.parquet")
            if os.path.exists(compacted_path):
                try:
                    dfc = pl.read_parquet(compacted_path)
                    dataframes.append((compacted_path, dfc))
                except Exception as e:
                    logger.warning(f"error leyendo compacted {compacted_path}: {e}")

            # parts
            try:
                for fname in os.listdir(folder):
                    if fname.startswith("part-") and fname.endswith(".parquet"):
                        path = os.path.join(folder, fname)
                        try:
                            dfp = pl.read_parquet(path)
                            dataframes.append((path, dfp))
                        except Exception as e:
                            logger.warning(f"error leyendo part {path}: {e}")
            except OSError:
                # el directorio puede desaparecer entre checks; ignorar
                pass

        current += timedelta(days=1)

    if not dataframes:
        return pl.DataFrame({
            "timestamp": [],
            "device_id": [],
            "heart_rate": [],
            "user_id": []
        })

    normalized = []
    for path, df in dataframes:
        try:
            # anadir columnas faltantes con nulls y forzar orden canonico
            for c in CANONICAL_COLS:
                if c not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(c))
            # seleccionar en orden canonico (ignora columnas extra)
            df = df.select(CANONICAL_COLS)

            # forzar tipos minimos (best-effort)
            try:
                df = df.with_columns(pl.col("timestamp_ms").cast(pl.Int64))
            except Exception:
                pass
            try:
                df = df.with_columns(pl.col("heart_rate").cast(pl.Int64))
            except Exception:
                pass
            try:
                df = df.with_columns(pl.col("device_id").cast(pl.Utf8))
                df = df.with_columns(pl.col("user_id").cast(pl.Utf8))
            except Exception:
                pass

            normalized.append(df)
        except Exception as e:
            logger.warning(f"Skipping {path} during normalization due to: {e}")

    if not normalized:
        return pl.DataFrame({
            "timestamp": [],
            "device_id": [],
            "heart_rate": [],
            "user_id": []
        })

    # concat seguro: todos tienen mismas columnas en mismo orden
    combined = pl.concat(normalized, how="vertical", rechunk=True)

    # filtrar por rango usando timestamp_ms 
    from app.util import datetime_to_epoch_ms
    start_utc = start.astimezone(timezone.utc) if start.tzinfo else start.replace(tzinfo=timezone.utc)
    end_utc   = end.astimezone(timezone.utc)   if end.tzinfo   else end.replace(tzinfo=timezone.utc)
    start_ms = datetime_to_epoch_ms(start_utc)
    end_ms = datetime_to_epoch_ms(end_utc)

    combined = combined.filter(
        (pl.col("timestamp_ms") >= start_ms) &
        (pl.col("timestamp_ms") <= end_ms)
    )

    # convertir timestamp_ms -> timestamp (Datetime UTC) despues del filtro
    # timestamp_ms ya esta en milisegundos, NO dividir por 1000
    combined = combined.with_columns(
        pl.col("timestamp_ms").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC").alias("timestamp")
    )

    # eliminar timestamp_ms ya que ahora tenemos timestamp
    combined = combined.drop("timestamp_ms")

    return combined


def deduplicate_same_device(df: pl.DataFrame) -> pl.DataFrame:
    """
    deduplica registros del mismo dispositivo con el mismo timestamp exacto.
    si hay multiples registros del mismo dispositivo en el mismo timestamp,
    calcula el promedio del heart_rate.
    """
    if df.is_empty():
        return df
    
    # agrupar por timestamp y device_id y promediar si hay duplicados
    df = df.group_by("timestamp", "device_id").agg([
        pl.col("heart_rate").mean().alias("heart_rate"),
        pl.col("user_id").first().alias("user_id")
    ])
    
    return df


def apply_device_priority(df: pl.DataFrame) -> pl.DataFrame:
    """
    aplica la prioridad de dispositivos: cuando hay multiples dispositivos
    para el mismo timestamp EXACTO, mantiene solo el de mayor prioridad (menor numero).
    """
    if df.is_empty():
        return df
    
    def get_priority(device_id: str) -> int:
        return DEVICE_PRIORITY.get(device_id, 999)  # si el dispositivo no esta definido tiene menor prioridad
    
    # creo columna de prioridad (1=mayor prioridad - medical grade)
    df = df.with_columns(
        pl.col("device_id").map_elements(get_priority, return_dtype=pl.Int64).alias("_priority")
    )
    
    # ordenar por timestamp y prioridad (menor numero de prioridad primero)
    df = df.sort("timestamp", "_priority") 
    
    # agrupar por timestamp EXACTO y mantener solo el de mayor prioridad (menor numero)
    df = df.group_by("timestamp").first()
    
    # eliminar columna auxiliar de prioridad
    df = df.drop("_priority")
    
    return df


def aggregate_by_minute(df: pl.DataFrame, device_id: Optional[str] = None) -> pl.DataFrame:
    """
    agrega los datos en buckets de 1 minuto calculando el promedio del heart_rate.
    resuelve conflictos por prioridad de dispositivos y luego agrega por minuto.
    """
    if df.is_empty():
        return df
    
    # paso 1: deduplicar registros del mismo dispositivo con el mismo timestamp exacto
    df = deduplicate_same_device(df)
    
    if df.is_empty():
        return df
    
    # paso 2: si hay filtro por device, solo filtrar. si no, aplicar prioridad entre dispositivos
    if device_id:
        df = df.filter(pl.col("device_id") == device_id)
    else:
        # aplicar prioridad para timestamps iguales (diferentes dispositivos)
        df = apply_device_priority(df)
    
    if df.is_empty():
        return df
    
    # paso 3: agregar por minuto (truncar timestamp al minuto y promediar)
    df = df.with_columns(
        pl.col("timestamp").dt.truncate("1m").alias("_timestamp_minute")
    )
    
    # ordenar antes del group_by para mantener orden (mas eficiente que sort despues)
    df = df.sort("_timestamp_minute", "device_id")
    
    # agrupar y agregar - maintain_order=True mantiene el orden de los grupos
    df = df.group_by("_timestamp_minute", "device_id", maintain_order=True).agg([
        pl.col("heart_rate").mean().alias("heart_rate"),
        pl.col("user_id").first().alias("user_id")
    ])
    
    # renombrar timestamp
    df = df.rename({"_timestamp_minute": "timestamp"}) 
    
    return df


def query_heart_rate_data(
    user_id: str,
    start: datetime,
    end: datetime,
    device_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    consulta los datos de heart rate de un usuario en un rango de tiempo.
    """
    # leer datos
    df = read_user_data(user_id, start, end)
    
    if df.is_empty():
        return []
    
    # agregar por minuto (tomar promedio) + handle prioridad
    df = aggregate_by_minute(df, device_id)
    
    if df.is_empty():
        return []
    
    # formatear timestamp a ISO 8601
    df = df.with_columns(
        pl.col("timestamp").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("timestamp")
    )
    
    # convertir heart_rate a entero
    df = df.with_columns(
        pl.col("heart_rate").cast(pl.Int64).alias("heart_rate")
    )
    
    df = df.sort("timestamp")
    
    # convierto a lista de diccionarios
    result = df.select(["timestamp", "heart_rate", "device_id"]).to_dicts()
    
    return result

