import os
from redis import Redis
from rq import Worker, Queue
from app.batcher import start_batcher_thread
from app.storage import DATA_DIR
from app.logger import get_logger, setup_logging

# Inicializar logging ANTES de cualquier uso del logger
setup_logging()

logger = get_logger(__name__)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
QUEUES = ["heartbeat"]

def main():
    logger.info("Iniciando worker RQ y batcher...")
    
    # Iniciar batcher thread (daemon)
    start_batcher_thread()
    logger.info("Batcher thread iniciado")

    # Conexión Redis para RQ
    redis_conn = Redis.from_url(REDIS_URL)
    logger.info(f"Conectado a Redis: {REDIS_URL}")
    logger.info(f"Escuchando queues: {QUEUES}")

    # Create worker with Redis connection and start listening to queues
    worker = Worker(QUEUES, connection=redis_conn)
    logger.info("Worker iniciado, comenzando a procesar jobs...")
    worker.work(with_scheduler=False)

if __name__ == "__main__":
    main()
