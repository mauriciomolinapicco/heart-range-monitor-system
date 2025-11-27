#!/usr/bin/env python3
"""
Script simple para verificar cuántos registros hay por usuario en el año 2025.
"""
import os
import sys
import glob
import polars as pl
from datetime import datetime

# Agregar el directorio raíz al PYTHONPATH para que pueda encontrar el módulo 'app'
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from app.config import DATA_DIR

def count_records_for_user(user_id: str, year: int = 2025) -> int:
    """Cuenta los registros de un usuario en un año específico."""
    total = 0
    
    # Buscar todos los archivos parquet del usuario en el año
    # Formato: data/YYYY-MM-DD/user_{user_id}*.parquet
    pattern = os.path.join(DATA_DIR, f"{year}-*", f"user_{user_id}*.parquet")
    files = glob.glob(pattern)
    
    for file_path in files:
        try:
            df = pl.read_parquet(file_path)
            total += len(df)
        except Exception as e:
            print(f"⚠️  Error leyendo {file_path}: {e}")
    
    return total


def main():
    print("=" * 60)
    print("📊 CONTEO DE REGISTROS POR USUARIO - AÑO 2025")
    print("=" * 60)
    print(f"Directorio de datos: {DATA_DIR}\n")
    
    if not os.path.exists(DATA_DIR):
        print(f"❌ El directorio {DATA_DIR} no existe")
        return
    
    total_all = 0
    user_counts = {}
    
    # Contar registros para cada usuario
    for user_num in range(1, 11):
        user_id = f"user_{user_num}"
        count = count_records_for_user(user_id, year=2025)
        user_counts[user_id] = count
        total_all += count
        
        if count > 0:
            print(f"  {user_id:12} : {count:6,} registros")
        else:
            print(f"  {user_id:12} : {count:6,} registros")
    
    print("-" * 60)
    print(f"  {'TOTAL':12} : {total_all:6,} registros")
    print("=" * 60)
    
    # Mostrar resumen
    users_with_data = sum(1 for count in user_counts.values() if count > 0)
    print(f"\nUsuarios con datos: {users_with_data}/10")
    
    if total_all > 0:
        avg = total_all / users_with_data if users_with_data > 0 else 0
        print(f"Promedio por usuario (con datos): {avg:,.0f} registros")


if __name__ == "__main__":
    main()

