# app/storage.py
"""
Funciones para el manejo de archivos .parquet
Operaciones de escritura - locking - gestion de directorios
"""
import os
import fcntl
import tempfile
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import polars as pl
from app.logger import get_logger

logger = get_logger(__name__)

DATA_DIR = os.getenv("HEARTBEAT_DATA_DIR", "data")

DEVICE_PRIORITY = {
    "device_a": 1,  # highest - medical grade
    "device_b": 2,  # consumer wearable
}


def ensure_dir(path: str) -> None:
    """Crea el directorio si no existe."""
    os.makedirs(path, exist_ok=True)


def get_file_path(user_id: str, date_str: str, create_dir: bool = True) -> str:
    """
    Genera la ruta del archivo parquet para un usuario y fecha.
    
    Args:
        user_id: ID del usuario
        date_str: Fecha en formato YYYY-MM-DD
        
    Returns:
        Ruta completa del archivo: data/YYYY-MM-DD/user_{user_id}.parquet
    """
    dest_dir = os.path.join(DATA_DIR, date_str)
    if create_dir:
        ensure_dir(dest_dir)
    filename = f"user_{user_id}.parquet"
    return os.path.join(dest_dir, filename)


def atomic_write_parquet(df: pl.DataFrame, dest_path: str) -> None:
    """
    Escribe un DataFrame a Parquet de forma atómica.
    
    Args:
        df: DataFrame de Polars a escribir
        dest_path: Ruta destino del archivo
        
    Raises:
        Exception: Si falla la escritura
    """
    tmp_fd, tmp_path = tempfile.mkstemp(
        suffix=".parquet",
        dir=os.path.dirname(dest_path)
    )
    os.close(tmp_fd)
    try:
        # Escribir con compresión snappy para mejor rendimiento
        df.write_parquet(tmp_path, compression="snappy")
        # Reemplazo atómico (POSIX)
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
    """
    Agrega un nuevo registro al archivo Parquet existente de forma segura.
    Usa file locking para manejar escritura concurrente.
    Implementa retry en caso de conflictos de escritura.
    
    Args:
        new_record: Diccionario con los datos del nuevo registro
        file_path: Ruta del archivo Parquet
        max_retries: Número máximo de intentos en caso de error
        
    Raises:
        RuntimeError: Si falla después de todos los intentos
    """
    # Crear DataFrame con el nuevo registro
    new_df = pl.DataFrame([new_record])
    
    # Usar un archivo de lock separado para mayor robustez
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
                # Eliminar el archivo de lock después de liberarlo
                if os.path.exists(lock_file_path):
                    try:
                        os.remove(lock_file_path)
                    except Exception:
                        pass  # Otro proceso pudo haberlo eliminado
        except Exception as e:
            # Limpiar en caso de error
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
                # Último intento falló, lanzar excepción
                logger.error(f"append_to_parquet falló después de {max_retries} intentos - file: {file_path}, error: {e}", exc_info=True)
                raise RuntimeError(f"Error al escribir en {file_path} después de {max_retries} intentos: {e}") from e
            logger.warning(f"append_to_parquet reintento {attempt + 1}/{max_retries} - file: {file_path}, error: {e}")
            # Esperar un poco antes de reintentar (exponential backoff)
            time.sleep(0.01 * (2 ** attempt))


def read_user_data(user_id: str, start: datetime, end: datetime) -> pl.DataFrame:
    """
    Lee todos los archivos parquet de un usuario en el rango de fechas especificado.
    
    Args:
        user_id: ID del usuario
        start: Fecha/hora de inicio (ISO 8601)
        end: Fecha/hora de fin (ISO 8601)
        
    Returns:
        DataFrame de Polars con todos los registros del usuario en el rango
    """
    dataframes = []
    
    current_date = start.date()
    end_date = end.date()
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        # No crear directorio al consultar, solo verificar si existe
        file_path = get_file_path(user_id, date_str, create_dir=False)
        
        if os.path.exists(file_path):
            try:
                df = pl.read_parquet(file_path)
                dataframes.append(df)
            except Exception as e:
                # caso archivos corruptos/vacios/o con algun error
                logger.warning(f"Error al leer archivo parquet - file: {file_path}, error: {e}")
                pass
        
        current_date += timedelta(days=1)
    
    if not dataframes:
        # retorno df vacio manteniendo schema
        return pl.DataFrame({
            "device_id": [],
            "user_id": [],
            "timestamp": [],
            "heart_rate": []
        })
    

    combined_df = pl.concat(dataframes) 
    
    # Convertir timestamp a datetime si es string
    if combined_df["timestamp"].dtype == pl.Utf8:
        combined_df = combined_df.with_columns(
            pl.col("timestamp").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%z")
            .or_else(pl.col("timestamp").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ"))
        )
    
    # filtro por rango de tiempo (antes filtre por fecha, debido a la forma en la que se organizan los archivos, ahora filtro por timestamp)
    combined_df = combined_df.filter(
        (pl.col("timestamp") >= start) & (pl.col("timestamp") <= end)
    )
    
    return combined_df


def deduplicate_same_device(df: pl.DataFrame) -> pl.DataFrame:
    """
    Deduplica registros del mismo dispositivo con el mismo timestamp exacto.
    Si hay múltiples registros del mismo dispositivo en el mismo timestamp,
    calcula el promedio del heart_rate.
    
    Args:
        df: DataFrame con los datos
        
    Returns:
        DataFrame sin duplicados del mismo dispositivo
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
    Aplica la prioridad de dispositivos: cuando hay múltiples dispositivos
    para el mismo timestamp EXACTO, mantiene solo el de mayor prioridad (menor número).
    
    Args:
        df: DataFrame con los datos (ya deduplicado por dispositivo)
        
    Returns:
        DataFrame con conflictos resueltos por prioridad de timestamp exacto
    """
    if df.is_empty():
        return df
    
    def get_priority(device_id: str) -> int:
        return DEVICE_PRIORITY.get(device_id, 999)  # si el dispositivo no esta definido tiene menor prioridad
    
    # creo columna de prioridad (1=mayor prioridad - medical grade)
    df = df.with_columns(
        pl.col("device_id").map_elements(get_priority, return_dtype=pl.Int64).alias("_priority")
    )
    
    # Ordenar por timestamp y prioridad (menor número de prioridad primero)
    df = df.sort("timestamp", "_priority") 
    
    # Agrupar por timestamp EXACTO y mantener solo el de mayor prioridad (menor número)
    df = df.group_by("timestamp").first()
    
    # Eliminar columna auxiliar de prioridad
    df = df.drop("_priority")
    
    return df


def aggregate_by_minute(df: pl.DataFrame, device_id: Optional[str] = None) -> pl.DataFrame:
    """
    Agrega los datos en buckets de 1 minuto calculando el promedio del heart_rate.
    Resuelve conflictos por prioridad de dispositivos y luego agrega por minuto.
    
    Args:
        df: DataFrame con los datos
        device_id: Opcional, filtrar por device_id antes de agregar
        
    Returns:
        DataFrame agregado por minuto con promedio de heart_rate
    """
    if df.is_empty():
        return df
    
    # Paso 1: Deduplicar registros del mismo dispositivo con el mismo timestamp exacto
    df = deduplicate_same_device(df)
    
    if df.is_empty():
        return df
    
    # Paso 2: Si hay filtro por device, solo filtrar. Si no, aplicar prioridad entre dispositivos
    if device_id:
        df = df.filter(pl.col("device_id") == device_id)
    else:
        # aplicar prioridad para timestamps iguales (diferentes dispositivos)
        df = apply_device_priority(df)
    
    if df.is_empty():
        return df
    
    # Paso 3: Agregar por minuto (truncar timestamp al minuto y promediar)
    df = df.with_columns(
        pl.col("timestamp").dt.truncate("1m").alias("_timestamp_minute")
    )
    
    # Ordenar antes del group_by para mantener orden (más eficiente que sort después)
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
    Consulta los datos de heart rate de un usuario en un rango de tiempo.
    
    Args:
        user_id: ID del usuario
        start: Fecha/hora de inicio
        end: Fecha/hora de fin
        device_id: Opcional, filtrar por device_id
        
    Returns:
        Lista de diccionarios con los datos agregados por minuto
    """
    # leer datos
    df = read_user_data(user_id, start, end)
    
    if df.is_empty():
        return []
    
    # agregar por minuto (tomar promedio) + handle prioridad
    df = aggregate_by_minute(df, device_id)
    
    if df.is_empty():
        return []
    
    # Formatear timestamp a ISO 8601
    df = df.with_columns(
        pl.col("timestamp").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("timestamp")
    )
    
    # convertir heart_rate a entero
    df = df.with_columns(
        pl.col("heart_rate").cast(pl.Int64).alias("heart_rate")
    )
    
    # convierto a lista de diccionarios
    result = df.select(["timestamp", "heart_rate", "device_id"]).to_dicts()
    
    return result

