"""
load test script para el sistema de heart rate monitoring.
envia multiples requests concurrentes y verifica el rendimiento y la persistencia de datos.
"""
import requests
import time
import random
import statistics
import os
import glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple
import polars as pl

BASE_URL = "http://localhost:8000"
DATA_DIR = os.getenv("HEARTBEAT_DATA_DIR", "data")

# configuracion del test
NUM_REQUESTS = 10000
CONCURRENT_WORKERS = 20
BATCH_WAIT_TIME = 10  # segundos para esperar que el batcher procese


def generate_heartbeat() -> Dict:
    """genera un payload de heartbeat aleatorio."""
    user_id = f"user_{random.randint(1, 10)}"
    device_id = random.choice(["device_a", "device_b"])
    timestamp = datetime.utcnow().isoformat() + "Z"
    heart_rate = random.randint(30, 220)
    
    return {
        "device_id": device_id,
        "user_id": user_id,
        "timestamp": timestamp,
        "heart_rate": heart_rate
    }


def send_request() -> Tuple[bool, float, Dict, str]:
    """envia un request y retorna (success, elapsed_time, payload, error)."""
    payload = generate_heartbeat()
    start_time = time.time()
    
    try:
        response = requests.post(
            f"{BASE_URL}/metrics/heart-rate",
            json=payload,
            timeout=10
        )
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            return (True, elapsed, payload, None)
        else:
            return (False, elapsed, payload, f"Status {response.status_code}: {response.text}")
    except Exception as e:
        elapsed = time.time() - start_time
        return (False, elapsed, payload, str(e))


def count_records_in_files(user_id: str, date_str: str) -> int:
    """cuenta los registros en los archivos parquet para un usuario y fecha."""
    user_dir = os.path.join(DATA_DIR, date_str, f"user-{user_id}")
    
    if not os.path.isdir(user_dir):
        return 0
    
    total_count = 0
    
    # leer compacted.parquet si existe
    compacted_path = os.path.join(user_dir, "compacted.parquet")
    if os.path.exists(compacted_path):
        try:
            df = pl.read_parquet(compacted_path)
            total_count += len(df)
        except Exception as e:
            print(f"⚠️  error leyendo compacted {compacted_path}: {e}")
    
    # leer todos los part-*.parquet
    pattern = os.path.join(user_dir, "part-*.parquet")
    for file_path in glob.glob(pattern):
        try:
            df = pl.read_parquet(file_path)
            total_count += len(df)
        except Exception as e:
            print(f"⚠️  error leyendo part {file_path}: {e}")
    
    return total_count


def wait_for_batch_processing(expected_count: int, date_str: str, max_wait: int = 60) -> int:
    """espera a que el batcher procese los datos y retorna el conteo final."""
    print(f"\n⏳ esperando que el batcher procese los datos (max {max_wait}s)...")
    print(f"   esperando {expected_count} registros...")
    
    start_time = time.time()
    last_count = 0
    check_interval = 2
    
    while time.time() - start_time < max_wait:
        time.sleep(check_interval)
        
        current_count = 0
        for user_id in [f"user_{i}" for i in range(1, 11)]:
            current_count += count_records_in_files(user_id, date_str)
        
        elapsed = int(time.time() - start_time)
        progress = (current_count / expected_count * 100) if expected_count > 0 else 0
        
        if current_count != last_count:
            print(f"   {elapsed}s: {current_count}/{expected_count} registros ({progress:.1f}%) - +{current_count - last_count}")
            last_count = current_count
            
            if current_count >= expected_count * 0.95:
                print(f"   ✅ procesamiento completo ({current_count}/{expected_count})")
                break
        elif elapsed % 10 == 0:
            print(f"   {elapsed}s: {current_count}/{expected_count} registros ({progress:.1f}%) - sin cambios")
    
    return last_count


def run_load_test():
    """ejecuta el test de carga completo."""
    print("=" * 70)
    print("🚀 LOAD TEST - Heart Rate Monitoring System")
    print("=" * 70)
    print(f"configuracion:")
    print(f"  - requests totales: {NUM_REQUESTS}")
    print(f"  - workers concurrentes: {CONCURRENT_WORKERS}")
    print(f"  - URL: {BASE_URL}")
    print(f"  - data directory: {DATA_DIR}")
    print()
    
    # verificar que el servidor este disponible
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print(f"❌ servidor no esta saludable: {response.status_code}")
            return
        print("✅ servidor saludable")
    except Exception as e:
        print(f"❌ no se puede conectar al servidor: {e}")
        print(f"   asegurate de que el servidor este corriendo (docker-compose up)")
        return
    
    print("\n" + "=" * 70)
    print("📤 enviando requests...")
    print("=" * 70)
    
    start_time = time.time()
    results = []
    errors = []
    payloads_sent = []
    
    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        # enviar todos los requests
        futures = [executor.submit(send_request) for _ in range(NUM_REQUESTS)]
        
        completed = 0
        for future in as_completed(futures):
            success, elapsed, payload, error = future.result()
            results.append({
                "success": success,
                "elapsed": elapsed,
                "payload": payload
            })
            payloads_sent.append(payload)
            
            if success:
                completed += 1
            else:
                errors.append({"payload": payload, "error": error})
            
            # mostrar progreso cada 100 requests
            if len(results) % 100 == 0:
                success_rate = (completed / len(results)) * 100
                print(f"   progreso: {len(results)}/{NUM_REQUESTS} - success: {success_rate:.1f}%")
    
    total_time = time.time() - start_time
    
    # estadisticas de requests
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]
    
    print("\n" + "=" * 70)
    print("📊 resultados de requests")
    print("=" * 70)
    print(f"total requests: {len(results)}")
    print(f"exitosos: {len(successful)} ({len(successful)/len(results)*100:.1f}%)")
    print(f"fallidos: {len(failed)} ({len(failed)/len(results)*100:.1f}%)")
    print(f"tiempo total: {total_time:.2f}s")
    print(f"throughput: {len(results)/total_time:.1f} req/s")
    
    if successful:
        latencies = [r["elapsed"] for r in successful]
        print(f"\nlatencia (exitosas):")
        print(f"  min: {min(latencies)*1000:.1f}ms")
        print(f"  max: {max(latencies)*1000:.1f}ms")
        print(f"  promedio: {statistics.mean(latencies)*1000:.1f}ms")
        print(f"  mediana: {statistics.median(latencies)*1000:.1f}ms")
        if len(latencies) > 1:
            sorted_latencies = sorted(latencies)
            p95_idx = int(len(sorted_latencies) * 0.95)
            p99_idx = int(len(sorted_latencies) * 0.99)
            print(f"  p95: {sorted_latencies[p95_idx]*1000:.1f}ms")
            print(f"  p99: {sorted_latencies[p99_idx]*1000:.1f}ms")
    
    if errors:
        print(f"\nerrores encontrados ({len(errors)}):")
        error_types = {}
        for e in errors:
            error_msg = e["error"] or "Unknown"
            error_types[error_msg] = error_types.get(error_msg, 0) + 1
        for error, count in error_types.items():
            print(f"  {error}: {count}")
    
    # esperar a que el batcher procese los datos
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    print(f"\n" + "=" * 70)
    print("💾 verificando persistencia de datos...")
    print("=" * 70)
    
    time.sleep(BATCH_WAIT_TIME)  # dar tiempo inicial al batcher
    total_written = wait_for_batch_processing(len(successful), date_str, max_wait=120)
    
    print(f"\ndesglose por usuario:")
    user_counts = {}
    for user_id in [f"user_{i}" for i in range(1, 11)]:
        count = count_records_in_files(user_id, date_str)
        user_counts[user_id] = count
        if count > 0:
            print(f"  {user_id}: {count} registros")
    
    print(f"\ntotal registros escritos en archivos: {total_written}")
    print(f"registros esperados: {len(successful)}")
    
    # resultado final
    print("\n" + "=" * 70)
    if total_written >= len(successful) * 0.95:
        print("✅ test exitoso")
        print(f"   {total_written}/{len(successful)} registros escritos ({total_written/len(successful)*100:.1f}%)")
    elif total_written >= len(successful) * 0.80:
        print("⚠️  test parcialmente exitoso")
        print(f"   {total_written}/{len(successful)} registros escritos ({total_written/len(successful)*100:.1f}%)")
        print("   puede que aun se esten procesando. espera unos minutos y verifica de nuevo.")
    else:
        print("❌ test fallido")
        print(f"   solo {total_written}/{len(successful)} registros escritos ({total_written/len(successful)*100:.1f}%)")
        print(f"   diferencia: {len(successful) - total_written} registros faltantes")
        print("   revisa los logs del consumer para ver si hay errores")
    print("=" * 70)


if __name__ == "__main__":
    run_load_test()

