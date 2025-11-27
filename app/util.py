"""
Utilidades para conversión de fechas y timestamps.
"""
from datetime import datetime, timezone


def datetime_to_epoch_ms(dt: datetime) -> int:
    """
    Convierte un datetime a milisegundos desde epoch (UTC).
    
    Args:
        dt: datetime object (si no tiene tzinfo, se asume UTC)
        
    Returns:
        Milisegundos desde epoch como entero
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def epoch_ms_to_dt(ms: int) -> datetime:
    """
    Convierte milisegundos desde epoch a datetime UTC.
    
    Args:
        ms: Milisegundos desde epoch
        
    Returns:
        datetime object en UTC
    """
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def epoch_ms_to_date_str(ms: int) -> str:
    """
    Convierte milisegundos desde epoch a string de fecha (YYYY-MM-DD).
    
    Args:
        ms: Milisegundos desde epoch
        
    Returns:
        String de fecha en formato YYYY-MM-DD
    """
    dt = epoch_ms_to_dt(ms)
    return dt.strftime("%Y-%m-%d")

