"""
Configuración centralizada del sistema.
Todas las constantes y configuraciones del proyecto están definidas aquí.
"""
import os

# ============================================================================
# Redis Configuration
# ============================================================================
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
QUEUE_KEY = os.getenv("HEARTBEAT_QUEUE_KEY", "heartbeat:queue")
PROCESSING_KEY = os.getenv("HEARTBEAT_PROCESSING_KEY", "heartbeat:processing")
# BUFFER_KEY es el mismo que QUEUE_KEY (compartido entre producer y consumer)
BUFFER_KEY = QUEUE_KEY

# ============================================================================
# Storage Configuration
# ============================================================================
DATA_DIR = os.getenv("HEARTBEAT_DATA_DIR", "data")
ARCHIVE_DIR = os.getenv("HEARTBEAT_ARCHIVE_DIR", "archive")

# ============================================================================
# Consumer Configuration
# ============================================================================
MAX_BATCH = int(os.getenv("MAX_BATCH", "400"))
MAX_BATCH_TIME = float(os.getenv("MAX_BATCH_TIME", "5.0"))  # segundos
BRPOP_TIMEOUT = int(os.getenv("BRPOP_TIMEOUT", "1"))  # segundos para BRPOPLPUSH

# ============================================================================
# Compacter Configuration
# ============================================================================
SLEEP_SECONDS = int(os.getenv("COMPACT_SLEEP_SECONDS", "300"))  # 5 minutos
MIN_PARTS_TO_COMPACT = int(os.getenv("MIN_PARTS_TO_COMPACT", "5"))

# ============================================================================
# Device Priority Configuration
# ============================================================================
DEVICE_PRIORITY = {
    "device_a": 1,  # highest - medical grade
    "device_b": 2,  # consumer wearable
}

# ============================================================================
# Schema Configuration
# ============================================================================
CANONICAL_COLS = ["timestamp_ms", "heart_rate", "device_id", "user_id"]

# ============================================================================
# Test/Development Configuration
# ============================================================================
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")

