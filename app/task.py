import json
from datetime import datetime
import os

DATA_PATH = "/data/processed.log"

def process_heartbeat(payload: dict):
    """
    Función consumida por RQ Worker.
    """
    record = {
        "received_at": datetime.utcnow().isoformat() + "Z",
        "payload": payload
    }

    os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)

    with open(DATA_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    return {"status": "ok", "saved": True}
