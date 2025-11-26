#!/usr/bin/env python3
import os, time, uuid, shutil
import polars as pl
from app.logger import get_logger, setup_logging
from app.storage import atomic_write_parquet

# Inicializar logging
setup_logging()
logger = get_logger(__name__)

DATA_DIR = "data"
ARCHIVE_DIR = "archive"
SLEEP_SECONDS = 300          # 5 min
MIN_PARTS_TO_COMPACT = 5
DEVICE_PRIORITY = {"device_a": 1, "device_b": 2}


# Helpers para listar directorios y archivos
def list_user_date_dirs():
    """Lista todos los pares (user_id, date) de directorios que existen."""
    out = []
    if not os.path.isdir(DATA_DIR):
        return out
    for date_name in os.listdir(DATA_DIR):
        date_path = os.path.join(DATA_DIR, date_name)
        if not os.path.isdir(date_path):
            continue
        for entry in os.listdir(date_path):
            # Buscar formato user-{user_id}
            if entry.startswith("user-"):
                user_id = entry.split("-", 1)[1]
                out.append((user_id, date_name))
    return out

def parts_for(user_id, date):
    """Lista todos los archivos part-*.parquet para un usuario y fecha."""
    folder = os.path.join(DATA_DIR, date, f"user-{user_id}")
    if not os.path.isdir(folder):
        return []
    return sorted([
        os.path.join(folder, f) for f in os.listdir(folder)
        if f.startswith("part-") and f.endswith(".parquet")
    ])


# canonical schema que queremos en compacted.parquet
CANONICAL_COLS = ["timestamp_ms", "heart_rate", "device_id", "user_id"]

def normalize_df_for_merge(df: pl.DataFrame) -> pl.DataFrame:
    """
    Asegura que el dataframe tenga las columnas CANONICAL_COLS en ese orden.
    Coercea tipos simples: timestamp_ms -> Int64, heart_rate -> Int64, device_id/user_id -> Utf8.
    """
    # agregar columnas faltantes con null
    for c in CANONICAL_COLS:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))

    # forzar tipos mínimos (best-effort)
    try:
        df = df.with_columns(
            pl.col("timestamp_ms").cast(pl.Int64).alias("timestamp_ms")
        )
    except Exception:
        pass
    try:
        df = df.with_columns(pl.col("heart_rate").cast(pl.Int64).alias("heart_rate"))
    except Exception:
        pass
    try:
        df = df.with_columns(pl.col("device_id").cast(pl.Utf8).alias("device_id"))
    except Exception:
        pass
    try:
        df = df.with_columns(pl.col("user_id").cast(pl.Utf8).alias("user_id"))
    except Exception:
        pass

    # seleccionar columnas en el orden canónico (ignorando extras)
    return df.select(CANONICAL_COLS)

def merge_parts(user_id, date, parts):
    folder = os.path.join(DATA_DIR, date, f"user-{user_id}")
    compacted_path = os.path.join(folder, "compacted.parquet")

    dfs = []
    # leer compacted existente y normalizar
    if os.path.exists(compacted_path):
        try:
            dfc = pl.read_parquet(compacted_path)
            dfs.append(normalize_df_for_merge(dfc))
        except Exception as e:
            logger.warning(f"Could not read existing compacted {compacted_path}: {e}")

    # leer parts snapshot y normalizarlos
    readable_parts = []
    for p in parts:
        try:
            dfp = pl.read_parquet(p)
            dfs.append(normalize_df_for_merge(dfp))
            readable_parts.append(p)
        except Exception as e:
            logger.warning(f"Skipping unreadable part {p}: {e}")

    if not dfs:
        logger.info("No readable dfs to merge")
        return

    # concat seguro
    df = pl.concat(dfs, how="vertical", rechunk=True)

    # aplicar device priority y dedupe por timestamp_ms
    pr = pl.DataFrame({"device_id": list(DEVICE_PRIORITY.keys()), "priority": list(DEVICE_PRIORITY.values())})
    df = df.join(pr, on="device_id", how="left").with_columns(pl.col("priority").fill_null(999))

    df = (
        df.sort(["timestamp_ms", "priority"])
          .group_by("timestamp_ms")
          .agg([
              pl.first("heart_rate").alias("heart_rate"),
              pl.first("device_id").alias("device_id"),
              pl.first("user_id").alias("user_id"),
          ])
    )

    # Asegurar orden canónico en el resultado final (timestamp_ms primero)
    df = df.select(["timestamp_ms", "heart_rate", "device_id", "user_id"])

    # escribir compacted atómico (usa tu helper)
    try:
        atomic_write_parquet(df, compacted_path)
        logger.info(f"Wrote compacted {compacted_path} rows={df.height}")
    except Exception:
        logger.exception("Failed to write compacted file")
        return

    # mover solo los parts procesados al archive
    for p in readable_parts:
        try:
            rel = os.path.relpath(p, DATA_DIR)
            dest = os.path.join(ARCHIVE_DIR, rel + ".done")
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.move(p, dest)
        except Exception:
            logger.exception("Failed archiving %s", p)

# Bucle principal
def main():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    logger.info("Compact loop started")
    while True:
        for user_id, date in list_user_date_dirs():
            folder = os.path.join(DATA_DIR, date, f"user-{user_id}")
            if not os.path.isdir(folder):
                continue

            parts = parts_for(user_id, date)
            if len(parts) >= MIN_PARTS_TO_COMPACT:
                print(f"[COMPACT] {user_id}/{date} - merging {len(parts)} parts")
                merge_parts(user_id, date, parts)

        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
