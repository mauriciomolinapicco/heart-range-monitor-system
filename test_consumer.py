# ex url: http://localhost:8000/metrics/heart-rate?user_id=user_1&start=2024-01-15T10:00:00Z&end=2025-12-15T10:30:00Z
import requests
import time

total_time = 0.0

for i in range(1,11):
    start = time.time()
    response = requests.get(f"http://localhost:8000/metrics/heart-rate?user_id=user_{i}&start=2024-01-15T10:00:00Z&end=2025-12-15T10:30:00Z")
    print(response.json())
    end = time.time()
    total_time += end - start


print(f"Tiempo promedio: {total_time / 10}")

# response = requests.get(f"http://localhost:8000/metrics/heart-rate?user_id=user_1&start=2020-01-15T10:00:00Z&end=2020-01-16T10:00:00Z")
# print(response.json())