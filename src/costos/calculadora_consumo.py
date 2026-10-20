"""Calculadora de consumo GCP del primer año para la arquitectura de producción de Lumina.

Genera docs/entrega1/calculadora_consumo.md (tablas para el documento) y .csv.
Todos los supuestos están en SUPUESTOS y PRECIOS; cambiar un valor y volver a correr:
    python src/costos/calculadora_consumo.py

Precios de lista us-central1 (USD, on-demand, oct-2026), verificados en las páginas de
precios de Google Cloud; validar con la Google Cloud Pricing Calculator antes de usar.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

HORAS_MES = 730
DIAS_MES = 30.4

# ---------------------------------------------------------------- supuestos del negocio
SUPUESTOS = {
    "clientes": 9_000_000,            # caso: más de 9 millones de clientes
    "penetracion_medidor": 0.89,      # caso: 89 % con medidor inteligente
    "kwh_dia_hogar": 10.28,           # medido en el dataset (fact_consumo_diario)
    "bytes_mensaje_kafka": 100,       # Avro + clave + headers por lectura
    "factor_rafaga": 10,              # concentradores envían en el minuto :00/:30
    "retencion_kafka_dias": 7,
    "bytes_lectura_parquet": 5.6,     # medido: gold/fact_consumo_30min
    "bytes_lectura_raw": 15.0,        # Avro comprimido en la capa raw (supuesto)
    "bytes_lectura_bq": 53.0,         # medido: BigQuery lógico, fact_consumo_30min
    "dcu_h_por_millon_lecturas": 0.064,  # medido: silver+gold 167.8 M lecturas = 10.8 DCU-h
    "factor_reproceso_batch": 1.3,    # reentrenamientos, backfills, pruebas
    "tib_consultas_mes": 10,          # BI/OLAP sobre agregados (on-demand)
    "solicitudes_api_mes_m": 30,      # millones de requests a la API (app + front-end)
    "gib_logs_mes": 200,
    "gb_egreso_mes": 150,
    "dias_lectura_1min_en_bq": 90,    # escenario 1 min: solo 90 días de detalle en BQ
}

# ---------------------------------------------------------------- precios de lista
PRECIOS = {
    "kafka_dcu_h": 0.09,              # Managed Service for Apache Kafka (1 vCPU=0.6, 1 GiB=0.1 DCU)
    "kafka_storage_gib_mes": 0.17,    # se factura 100 GiB locales por vCPU
    "interzona_gib": 0.01,            # réplica RF=3 -> 2 zonas extra
    "n2_standard_4_h": 0.1942,        # Compute Engine
    "dataproc_vcpu_h": 0.01,          # tarifa Dataproc sobre VMs (clúster de streaming)
    "spark_serverless_dcu_h": 0.06,   # Managed Service for Apache Spark, serverless estándar
    "gcs_dual_region_gb_mes": 0.036,  # nam4 (curated + checkpoints, para DR)
    "gcs_standard_gb_mes": 0.020,     # us-central1 (raw)
    "bq_storage_gib_mes": 0.016,      # mezcla activo 0.02 / largo plazo 0.01 (lógico)
    "bq_tib_consulta": 6.25,
    "bigtable_nodo_h": 0.65,
    "bigtable_ssd_gib_mes": 0.17,
    "cloud_run_millon_req": 0.40,
    "cloud_run_cpu_mes": 45.0,        # ~2 instancias mínimas + picos (estimado)
    "composer_mes": 250.0,            # Managed Service for Apache Airflow (Composer 3) pequeño
    "logging_gib": 0.50,
    "egreso_gb": 0.12,
}


@dataclass
class Linea:
    capa: str
    servicio: str
    supuesto: str
    usd_anio: float


def escenario(minutos_lectura: int) -> tuple[list[Linea], dict]:
    s, p = SUPUESTOS, PRECIOS
    medidores = s["clientes"] * s["penetracion_medidor"]
    lect_dia = medidores * 24 * 60 / minutos_lectura
    lect_anio = lect_dia * 365
    mb_s = lect_dia * s["bytes_mensaje_kafka"] / 86_400 / 1e6
    mb_s_pico = mb_s * s["factor_rafaga"]
    meses = range(1, 13)

    # Kafka gestionado: 1 vCPU por cada ~15 MB/s de pico, mínimo 3 (una por zona), 4 GiB/vCPU
    vcpu_kafka = max(3, 3 * round(mb_s_pico / 15 / 3 + 0.5))
    dcu_kafka = vcpu_kafka * 0.6 + vcpu_kafka * 4 * 0.1
    gib_retencion = lect_dia * s["bytes_mensaje_kafka"] * s["retencion_kafka_dias"] / 2**30
    gib_local = max(vcpu_kafka * 100, gib_retencion)
    kafka = (dcu_kafka * p["kafka_dcu_h"] * HORAS_MES + gib_local * p["kafka_storage_gib_mes"]
             + lect_dia * s["bytes_mensaje_kafka"] / 2**30 * 2 * p["interzona_gib"] * DIAS_MES) * 12

    # Spark Structured Streaming 24/7: 1 maestro + N workers n2-standard-4
    workers = max(2, round(mb_s_pico / 20) + 2)
    nodos = workers + 1
    streaming = nodos * (p["n2_standard_4_h"] + 4 * p["dataproc_vcpu_h"]) * HORAS_MES * 12

    # Batch diario incremental en Spark serverless (rendimiento medido en el taller)
    batch = (lect_dia / 1e6 * s["dcu_h_por_millon_lecturas"] * s["factor_reproceso_batch"]
             * p["spark_serverless_dcu_h"] * 365)

    # Almacenamiento: crece linealmente durante el año (se suma mes a mes)
    def acumulado(bytes_por_lectura: float, precio: float, unidad: float = 1e9) -> float:
        return sum(lect_anio * m / 12 * bytes_por_lectura / unidad * precio for m in meses)

    gcs = (acumulado(s["bytes_lectura_raw"], p["gcs_standard_gb_mes"])
           + acumulado(2 * s["bytes_lectura_parquet"], p["gcs_dual_region_gb_mes"]))  # silver+gold

    if minutos_lectura == 30:
        bq_storage = acumulado(s["bytes_lectura_bq"] * 1.15, p["bq_storage_gib_mes"], 2**30)
        nota_bq = "detalle 30 min + 15 % de agregados, crecimiento lineal"
    else:  # 1 min: detalle solo 90 días en BQ; histórico en el lake; agregados a 30 min
        detalle = lect_dia * s["dias_lectura_1min_en_bq"] * s["bytes_lectura_bq"] / 2**30
        agregado_30 = lect_anio / 30 * s["bytes_lectura_bq"] * 1.15
        bq_storage = (detalle * p["bq_storage_gib_mes"] * 12
                      + sum(agregado_30 * m / 12 / 2**30 * p["bq_storage_gib_mes"] for m in meses))
        nota_bq = f"detalle 1 min solo {s['dias_lectura_1min_en_bq']} días + agregados 30 min"
    bq_query = max(0, s["tib_consultas_mes"] - 1) * p["bq_tib_consulta"] * 12

    # Bigtable: serving de la app (últimos 7 días por medidor), 2 clústeres replicados
    nodos_bt = 2 if minutos_lectura == 30 else 6
    gib_bt = lect_dia * 7 * 50 / 2**30 * 2
    bigtable = (nodos_bt * p["bigtable_nodo_h"] * HORAS_MES + gib_bt * p["bigtable_ssd_gib_mes"]) * 12

    api = (s["solicitudes_api_mes_m"] * p["cloud_run_millon_req"] + p["cloud_run_cpu_mes"]) * 12
    airflow = p["composer_mes"] * 12
    ops = (s["gib_logs_mes"] * p["logging_gib"] + s["gb_egreso_mes"] * p["egreso_gb"]) * 12

    lineas = [
        Linea("Ingesta", "Managed Service for Apache Kafka",
              f"{vcpu_kafka} vCPU, RF=3, {s['retencion_kafka_dias']} días; {mb_s:.1f} MB/s prom., {mb_s_pico:.0f} MB/s pico", kafka),
        Linea("Cómputo", "Spark Structured Streaming (Dataproc, 24/7)",
              f"{nodos} nodos n2-standard-4", streaming),
        Linea("Cómputo", "Spark batch serverless (diario)",
              f"{lect_dia / 1e6:,.0f} M lecturas/día × {s['dcu_h_por_millon_lecturas']} DCU-h/M", batch),
        Linea("Almacenamiento", "Cloud Storage (lake raw + curated dual-region)",
              f"{lect_anio * (s['bytes_lectura_raw'] + 2 * s['bytes_lectura_parquet']) / 1e12:,.1f} TB al cierre del año", gcs),
        Linea("Almacenamiento", "BigQuery almacenamiento", nota_bq, bq_storage),
        Linea("Almacenamiento", "Bigtable (serving app, 2 clústeres)",
              f"{nodos_bt} nodos, {gib_bt:,.0f} GiB SSD", bigtable),
        Linea("Exposición", "BigQuery consultas (OLAP / Looker Studio)",
              f"{s['tib_consultas_mes']} TiB/mes on-demand (1 TiB gratis)", bq_query),
        Linea("Exposición", "Cloud Run (API REST/WebSocket)",
              f"{s['solicitudes_api_mes_m']} M requests/mes", api),
        Linea("Transversal", "Managed Service for Apache Airflow (Composer 3)", "entorno pequeño", airflow),
        Linea("Transversal", "Logging, Monitoring y egreso",
              f"{s['gib_logs_mes']} GiB logs/mes, {s['gb_egreso_mes']} GB egreso/mes", ops),
    ]
    total = sum(l.usd_anio for l in lineas)
    kwh_anio = medidores * s["kwh_dia_hogar"] * 365
    metricas = {
        "medidores": medidores,
        "lecturas_dia": lect_dia,
        "lecturas_anio": lect_anio,
        "total_usd_anio": total,
        "usd_medidor_mes": total / medidores / 12,
        "usd_millon_lecturas": total / (lect_anio / 1e6),
        "usd_mwh_distribuido": total / (kwh_anio / 1000),
        "twh_anio": kwh_anio / 1e9,
    }
    return lineas, metricas


def tabla(lineas: list[Linea], m: dict, titulo: str) -> str:
    filas = [f"### {titulo}", "",
             "| Capa | Servicio | Dimensionamiento | USD primer año |",
             "|---|---|---|---:|"]
    filas += [f"| {l.capa} | {l.servicio} | {l.supuesto} | {l.usd_anio:,.0f} |" for l in lineas]
    filas += [f"| **Total** | | | **{m['total_usd_anio']:,.0f}** |", "",
              "| Métrica unitaria | Valor |", "|---|---:|",
              f"| Lecturas por día | {m['lecturas_dia'] / 1e6:,.0f} M |",
              f"| Costo por medidor al mes | US${m['usd_medidor_mes']:.4f} |",
              f"| Costo por millón de lecturas procesadas | US${m['usd_millon_lecturas']:.3f} |",
              f"| Costo de plataforma por MWh distribuido ({m['twh_anio']:.1f} TWh/año) | US${m['usd_mwh_distribuido']:.4f} |",
              ""]
    return "\n".join(filas)


def main() -> None:
    salida = Path(__file__).resolve().parents[2] / "docs" / "entrega1"
    salida.mkdir(parents=True, exist_ok=True)
    md, filas_csv = [], []
    for minutos, titulo in ((30, "Escenario base: lectura cada 30 min"),
                            (1, "Escenario de crecimiento: lectura cada 1 min")):
        lineas, m = escenario(minutos)
        md.append(tabla(lineas, m, titulo))
        filas_csv += [{"escenario_min": minutos, "capa": l.capa, "servicio": l.servicio,
                       "supuesto": l.supuesto, "usd_anio": round(l.usd_anio, 2)} for l in lineas]
    (salida / "calculadora_consumo.md").write_text("\n".join(md), encoding="utf-8", newline="\n")
    with open(salida / "calculadora_consumo.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(filas_csv[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(filas_csv)
    print("\n".join(md))


if __name__ == "__main__":
    main()
