"""Utilidades compartidas por los jobs batch de Lumina (silver.py, gold.py)."""
from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame, SparkSession

ZONA_LONDRES = "Europe/London"


def spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        # BigQuery lee TIMESTAMP_MICROS de forma nativa (INT96 es legado).
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def parse_args(description: str) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--entrada", required=True, help="Prefijo de entrada (gs://... o ruta local)")
    p.add_argument("--salida", required=True, help="Prefijo de salida (gs://... o ruta local)")
    p.add_argument("--muestra-bloques", type=int, default=0,
                   help="Procesa solo los N primeros bloques de medidores (0 = todos)")
    p.add_argument("--entrada-streaming", default="",
                   help="Bronze escrito por el streaming (gs://<raw>/streaming/lecturas); vacío = no se usa")
    return p.parse_args()


def escribir(df: DataFrame, ruta: str, particion: str | None = None, archivos: int | None = None) -> None:
    """Escribe Parquet sobrescribiendo y reporta filas y duración."""
    inicio = time.time()
    if archivos:
        df = df.repartition(archivos, particion) if particion else df.coalesce(archivos)
    writer = df.write.mode("overwrite").option("compression", "snappy")
    if particion:
        writer = writer.partitionBy(particion)
    writer.parquet(ruta)
    filas = df.sparkSession.read.parquet(ruta).count()
    print(f"[lumina] {ruta}: {filas:,} filas en {time.time() - inicio:.0f}s", flush=True)
