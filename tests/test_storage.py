"""
Pruebas simples de escritura y lectura de datos.
"""
import pytest
import os
import tempfile
import shutil
from datetime import datetime
from app.storage import append_to_parquet, get_file_path, query_heart_rate_data, read_user_data
from app.tasks import process_heartbeat


@pytest.fixture
def temp_data_dir():
    """Crea un directorio temporal para las pruebas."""
    temp_dir = tempfile.mkdtemp()
    original_dir = os.getenv("HEARTBEAT_DATA_DIR")
    os.environ["HEARTBEAT_DATA_DIR"] = temp_dir
    yield temp_dir
    # Limpiar
    shutil.rmtree(temp_dir)
    if original_dir:
        os.environ["HEARTBEAT_DATA_DIR"] = original_dir
    elif "HEARTBEAT_DATA_DIR" in os.environ:
        del os.environ["HEARTBEAT_DATA_DIR"]


def test_write_and_read_simple(temp_data_dir):
    """Prueba simple: escribir un registro y leerlo."""
    user_id = "test_user_1"
    date_str = "2025-01-15"
    
    record = {
        "device_id": "device_a",
        "user_id": user_id,
        "timestamp": "2025-01-15T10:00:00Z",
        "heart_rate": 75
    }
    
    file_path = get_file_path(user_id, date_str)
    append_to_parquet(record, file_path)
    
    start = datetime.fromisoformat("2025-01-15T10:00:00+00:00")
    end = datetime.fromisoformat("2025-01-15T10:01:00+00:00")
    df = read_user_data(user_id, start, end)
    
    assert len(df) >= 1
    assert df["user_id"][0] == user_id
    assert df["device_id"][0] == "device_a"
    assert df["heart_rate"][0] == 75


def test_write_multiple_and_query(temp_data_dir):
    """Prueba: escribir varios registros y consultarlos."""
    user_id = "mauricio"
    date_str = "2025-01-15"
    
    records = [
        {"device_id": "device_a", "user_id": user_id, "timestamp": "2025-01-15T10:00:00Z", "heart_rate": 70},
        {"device_id": "device_a", "user_id": user_id, "timestamp": "2025-01-15T10:01:00Z", "heart_rate": 75},
        {"device_id": "device_a", "user_id": user_id, "timestamp": "2025-01-15T10:02:00Z", "heart_rate": 80},
    ]
    
    file_path = get_file_path(user_id, date_str)
    for record in records:
        append_to_parquet(record, file_path)
    
    start = datetime.fromisoformat("2025-01-15T10:00:00+00:00")
    end = datetime.fromisoformat("2025-01-15T10:03:00+00:00")
    result = query_heart_rate_data(user_id, start, end)
    
    assert len(result) == 3
    assert result[0]["heart_rate"] == 70
    assert result[1]["heart_rate"] == 75
    assert result[2]["heart_rate"] == 80


def test_process_heartbeat_task(temp_data_dir):
    """Prueba: usar la función process_heartbeat."""
    payload = {
        "user_id": "test_user_3",
        "device_id": "device_b",
        "timestamp": "2025-01-15T11:00:00Z",
        "heart_rate": 85
    }
    
    result = process_heartbeat(payload)
    
    assert result["status"] == "appended"
    assert result["user_id"] == "test_user_3"
    
    start = datetime.fromisoformat("2025-01-15T11:00:00+00:00")
    end = datetime.fromisoformat("2025-01-15T11:01:00+00:00")
    data = query_heart_rate_data("test_user_3", start, end)
    
    assert len(data) == 1
    assert data[0]["heart_rate"] == 85

