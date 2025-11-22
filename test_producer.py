"""
vibe coded test script
note: must be running the server (docker-compose up)
"""
import requests
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "http://localhost:8000"


def generate_heartbeat(
    user_id: str = None,
    device_id: str = None,
    timestamp: str = None,
    heart_rate: int = None
) -> Dict:
    """Genera un payload de heartbeat con valores aleatorios o especificados."""
    if user_id is None:
        user_id = f"user_{random.randint(1, 10)}"
    
    if device_id is None:
        device_id = random.choice(["device_a", "device_b"])
    
    if timestamp is None:
        # Timestamp aleatorio en las últimas 24 horas
        now = datetime.utcnow()
        random_offset = timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        timestamp = (now - random_offset).isoformat() + "Z"
    
    if heart_rate is None:
        heart_rate = random.randint(30, 220)
    
    return {
        "device_id": device_id,
        "user_id": user_id,
        "timestamp": timestamp,
        "heart_rate": heart_rate
    }


def send_heartbeat(payload: Dict) -> tuple:
    """Envía un heartbeat y retorna (success, response_time, error)."""
    start_time = time.time()
    try:
        response = requests.post(
            f"{BASE_URL}/metrics/heart-rate",
            json=payload,
            timeout=5
        )
        response_time = time.time() - start_time
        
        if response.status_code == 200:
            return (True, response_time, None)
        else:
            return (False, response_time, f"Status {response.status_code}: {response.text}")
    except Exception as e:
        response_time = time.time() - start_time
        return (False, response_time, str(e))


def test_post_requests(num_requests: int, concurrent: int = 1, verbose: bool = False):
    """Envía múltiples requests POST."""
    print(f"\n🚀 Enviando {num_requests} requests POST (concurrent: {concurrent})...")
    
    payloads = [generate_heartbeat() for _ in range(num_requests)]
    
    results = {
        "success": 0,
        "errors": 0,
        "response_times": [],
        "errors_list": []
    }
    
    start_time = time.time()
    
    if concurrent == 1:
        # Secuencial
        for i, payload in enumerate(payloads, 1):
            success, resp_time, error = send_heartbeat(payload)
            results["response_times"].append(resp_time)
            
            if success:
                results["success"] += 1
                if verbose:
                    print(f"✅ [{i}/{num_requests}] Success - {resp_time:.3f}s")
            else:
                results["errors"] += 1
                results["errors_list"].append(error)
                print(f"❌ [{i}/{num_requests}] Error: {error}")
    else:
        # Concurrente
        with ThreadPoolExecutor(max_workers=concurrent) as executor:
            future_to_payload = {
                executor.submit(send_heartbeat, payload): (i, payload)
                for i, payload in enumerate(payloads, 1)
            }
            
            for future in as_completed(future_to_payload):
                i, payload = future_to_payload[future]
                try:
                    success, resp_time, error = future.result()
                    results["response_times"].append(resp_time)
                    
                    if success:
                        results["success"] += 1
                        if verbose:
                            print(f"✅ [{i}/{num_requests}] Success - {resp_time:.3f}s")
                    else:
                        results["errors"] += 1
                        results["errors_list"].append(error)
                        print(f"❌ [{i}/{num_requests}] Error: {error}")
                except Exception as e:
                    results["errors"] += 1
                    results["errors_list"].append(str(e))
                    print(f"❌ [{i}/{num_requests}] Exception: {e}")
    
    total_time = time.time() - start_time
    
    # Estadísticas
    print(f"\n📊 Estadísticas:")
    print(f"   Total requests: {num_requests}")
    print(f"   Exitosos: {results['success']} ({results['success']/num_requests*100:.1f}%)")
    print(f"   Errores: {results['errors']} ({results['errors']/num_requests*100:.1f}%)")
    print(f"   Tiempo total: {total_time:.2f}s")
    print(f"   Throughput: {num_requests/total_time:.2f} req/s")
    
    if results["response_times"]:
        avg_time = sum(results["response_times"]) / len(results["response_times"])
        min_time = min(results["response_times"])
        max_time = max(results["response_times"])
        print(f"   Tiempo respuesta promedio: {avg_time:.3f}s")
        print(f"   Tiempo respuesta mínimo: {min_time:.3f}s")
        print(f"   Tiempo respuesta máximo: {max_time:.3f}s")
    
    if results["errors_list"]:
        print(f"\n❌ Errores encontrados:")
        error_counts = {}
        for error in results["errors_list"]:
            error_counts[error] = error_counts.get(error, 0) + 1
        for error, count in error_counts.items():
            print(f"   {error}: {count} veces")


def test_get_request(user_id: str, start: str, end: str, device_id: str = None):
    """Prueba el endpoint GET."""
    print(f"\n🔍 Consultando datos para user_id={user_id}...")
    
    params = {
        "user_id": user_id,
        "start": start,
        "end": end
    }
    if device_id:
        params["device_id"] = device_id
    
    try:
        response = requests.get(f"{BASE_URL}/metrics/heart-rate", params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Consulta exitosa:")
            print(f"   User ID: {data['user_id']}")
            print(f"   Registros encontrados: {data['count']}")
            if data['count'] > 0:
                print(f"   Primer registro: {data['data'][0]}")
                if len(data['data']) > 1:
                    print(f"   Último registro: {data['data'][-1]}")
        else:
            print(f"❌ Error: Status {response.status_code}")
            print(f"   {response.text}")
    except Exception as e:
        print(f"❌ Error al consultar: {e}")


def test_validation():
    """Prueba las validaciones del endpoint."""
    print("\n🧪 Probando validaciones...")
    
    # Test 1: heart_rate fuera de rango (menor)
    print("\n1. Test: heart_rate < 30")
    payload = generate_heartbeat(heart_rate=25)
    response = requests.post(f"{BASE_URL}/metrics/heart-rate", json=payload)
    print(f"   Status: {response.status_code} (esperado: 422)")
    
    # Test 2: heart_rate fuera de rango (mayor)
    print("\n2. Test: heart_rate > 220")
    payload = generate_heartbeat(heart_rate=250)
    response = requests.post(f"{BASE_URL}/metrics/heart-rate", json=payload)
    print(f"   Status: {response.status_code} (esperado: 422)")
    
    # Test 3: heart_rate válido
    print("\n3. Test: heart_rate válido (75)")
    payload = generate_heartbeat(heart_rate=75)
    response = requests.post(f"{BASE_URL}/metrics/heart-rate", json=payload)
    print(f"   Status: {response.status_code} (esperado: 200)")


def main():
    # response = requests.post("http://localhost:8000/metrics/heart-rate", json={"device_id": "device_a", "user_id": "test_user", "timestamp": "2025-11-21T10:00:00Z", "heart_rate": 75})
    # print(response.json())
    # return
    global BASE_URL
    
    parser = argparse.ArgumentParser(description="Script de prueba para el endpoint de heart rate")
    parser.add_argument("--num", type=int, default=10, help="Número de requests POST a enviar (default: 10)")
    parser.add_argument("--concurrent", type=int, default=1, help="Número de requests concurrentes (default: 1)")
    parser.add_argument("--get", action="store_true", help="También probar endpoint GET")
    parser.add_argument("--validate", action="store_true", help="Probar validaciones")
    parser.add_argument("--verbose", "-v", action="store_true", help="Mostrar detalles de cada request")
    parser.add_argument("--url", type=str, default=BASE_URL, help=f"URL base del API (default: {BASE_URL})")
    
    args = parser.parse_args()
    BASE_URL = args.url
    
    print(f"🌐 Conectando a: {BASE_URL}")
    
    # Verificar que el servidor esté corriendo
    try:
        response = requests.get(f"{BASE_URL}/docs", timeout=2)
        print("✅ Servidor disponible")
    except Exception as e:
        print(f"❌ No se puede conectar al servidor: {e}")
        print("   Asegúrate de que el servidor esté corriendo en http://localhost:8000")
        return
    
    # Probar validaciones
    if args.validate:
        test_validation()
    
    # Enviar requests POST
    if args.num > 0:
        test_post_requests(args.num, args.concurrent, args.verbose)
    
    # Probar GET
    if args.get:
        # Consultar datos de un usuario de prueba
        now = datetime.utcnow()
        start = (now - timedelta(days=1)).isoformat() + "Z"
        end = now.isoformat() + "Z"
        test_get_request("user_1", start, end)


if __name__ == "__main__":
    main()

