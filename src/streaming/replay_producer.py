"""Reproduce lecturas históricas de medidores como un flujo en vivo hacia Kafka.

Lee una ventana de fact_consumo_30min (BigQuery) y emite cada media hora simulada
en `--segundos-intervalo` segundos reales. Para la demo puede inyectar fallas:
  --concentrador-caido Z3-T07   todos los medidores de ese transformador dejan de
                                enviar desde --caida-desde (simula un concentrador caído)
  --anomalias 20                multiplica por 10 el consumo de 20 lecturas (> 0.4 kWh) al azar

Tópicos: meter.readings (clave = lclid) y grid.demand (clave = "GB").
Ejemplo:
  python replay_producer.py --proyecto P --desde 2013-01-14 --dias 7 \
      --segundos-intervalo 2 --concentrador-caido Z3-T07 --anomalias 20
"""
from __future__ import annotations

import argparse
import json
import time

import numpy as np
import pandas as pd
from confluent_kafka import Producer
from google.cloud import bigquery


def argumentos() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--proyecto", required=True)
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--desde", default="2013-01-14", help="Fecha inicial (UTC)")
    p.add_argument("--dias", type=int, default=7)
    p.add_argument("--segundos-intervalo", type=float, default=2.0,
                   help="Segundos reales por cada media hora simulada")
    p.add_argument("--max-medidores", type=int, default=0, help="0 = todos")
    p.add_argument("--concentrador-caido", default="", help="Transformador que deja de enviar")
    p.add_argument("--caida-desde", default="2013-01-16 06:00:00")
    p.add_argument("--anomalias", type=int, default=0)
    p.add_argument("--semilla", type=int, default=42)
    return p.parse_args()


def cargar(args: argparse.Namespace):
    bq = bigquery.Client(project=args.proyecto)
    params = [
        bigquery.ScalarQueryParameter("desde", "DATE", args.desde),
        bigquery.ScalarQueryParameter("dias", "INT64", args.dias),
    ]
    filtro = "c.fecha >= @desde AND c.fecha < DATE_ADD(@desde, INTERVAL @dias DAY)"
    lecturas = bq.query(f"""
        SELECT c.lclid, c.ts_utc, c.kwh, h.transformador
        FROM `{args.proyecto}.lumina_curated.fact_consumo_30min` c
        JOIN `{args.proyecto}.lumina_curated.dim_hogar` h USING (lclid)
        WHERE {filtro} AND c.kwh IS NOT NULL
        ORDER BY c.ts_utc, c.lclid""",
        job_config=bigquery.QueryJobConfig(query_parameters=params)).to_dataframe()
    demanda = bq.query(f"""
        SELECT ts_utc, nd_mw, eolica_embebida_mw, solar_embebida_mw
        FROM `{args.proyecto}.lumina_curated.fact_demanda_nacional_30min` c
        WHERE {filtro}""",
        job_config=bigquery.QueryJobConfig(query_parameters=params)).to_dataframe()
    return lecturas, demanda.set_index("ts_utc")


def inyectar_fallas(lecturas, args: argparse.Namespace):
    rng = np.random.default_rng(args.semilla)
    if args.max_medidores:
        elegidos = rng.choice(lecturas.lclid.unique(), args.max_medidores, replace=False)
        lecturas = lecturas[lecturas.lclid.isin(elegidos)]
    if args.concentrador_caido:
        corte = (lecturas.transformador == args.concentrador_caido) & \
                (lecturas.ts_utc >= pd.Timestamp(args.caida_desde, tz="UTC"))
        afectados = lecturas.loc[corte, "lclid"].nunique()
        lecturas = lecturas[~corte]
        print(f"[falla] concentrador {args.concentrador_caido} caído desde {args.caida_desde}: "
              f"{afectados} medidores sin datos", flush=True)
    if args.anomalias:
        candidatos = lecturas.index[lecturas.kwh > 0.4]
        idx = rng.choice(candidatos, args.anomalias, replace=False)
        lecturas = lecturas.copy()
        lecturas.loc[idx, "kwh"] = lecturas.loc[idx, "kwh"] * 10
        for _, r in lecturas.loc[idx].sort_values("ts_utc").iterrows():
            print(f"[falla] anomalía inyectada: {r.lclid} {r.ts_utc:%Y-%m-%d %H:%M} kwh={r.kwh:.3f}", flush=True)
    return lecturas


def main() -> None:
    args = argumentos()
    lecturas, demanda = cargar(args)
    lecturas = inyectar_fallas(lecturas, args)
    print(f"[replay] {len(lecturas):,} lecturas de {lecturas.lclid.nunique():,} medidores, "
          f"{lecturas.ts_utc.nunique()} intervalos", flush=True)

    productor = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 50,
                          "compression.type": "lz4", "acks": "all"})
    enviados = 0
    for i, (ts, grupo) in enumerate(lecturas.groupby("ts_utc", sort=True)):
        inicio = time.monotonic()
        ts_txt = ts.strftime("%Y-%m-%d %H:%M:%S")
        for lclid, kwh in zip(grupo.lclid, grupo.kwh):
            productor.produce("meter.readings", key=lclid,
                              value=json.dumps({"lclid": lclid, "ts_utc": ts_txt, "kwh": round(float(kwh), 4)}))
            productor.poll(0)
        if ts in demanda.index:
            d = demanda.loc[ts]
            productor.produce("grid.demand", key="GB", value=json.dumps({
                "ts_utc": ts_txt, "nd_mw": float(d.nd_mw),
                "eolica_embebida_mw": float(d.eolica_embebida_mw),
                "solar_embebida_mw": float(d.solar_embebida_mw)}))
        productor.flush()
        enviados += len(grupo)
        if i % 12 == 0:
            print(f"[replay] {ts_txt} UTC · {len(grupo):,} lecturas · total {enviados:,}", flush=True)
        time.sleep(max(0.0, args.segundos_intervalo - (time.monotonic() - inicio)))
    print(f"[replay] fin: {enviados:,} lecturas enviadas", flush=True)


if __name__ == "__main__":
    main()
