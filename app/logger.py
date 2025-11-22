"""
Configuración de logging para la aplicación.
"""
import logging
import sys
from typing import Optional
import os

def setup_logging(log_level: Optional[str] = None) -> None:
    if log_level is None:
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    numeric_level = getattr(logging, log_level, logging.INFO)
    
    # Formato de log: timestamp - nivel - módulo - mensaje
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # Configurar logging básico
    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Reducir verbosidad de librerías externas
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("rq").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

