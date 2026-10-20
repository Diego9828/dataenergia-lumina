"""Ejemplo dummy: cómo fluye una lectura por la arquitectura Lambda de Lumina.

Datos ficticios (3 hogares, 4 medias horas por día) y solo pandas: no usa Spark ni GCP.
Reproduce en miniatura lo que hacen silver.py, gold.py, streaming_job.py y las vistas OLAP.
Correr:  python notebooks/flujo_dummy.py
"""
import pandas as pd

pd.set_option("display.width", 140)
HORAS = ["18:00", "18:30", "19:00", "19:30"]


def titulo(texto: str) -> None:
    print(f"\n{'=' * 100}\n{texto}\n{'=' * 100}")


def tabla(nombre: str, df: pd.DataFrame) -> None:
    print(f"\n-- {nombre}")
    print(df.to_string(index=False))


# ---------------------------------------------------------------- fuentes
titulo("0 · FUENTES")
hogares = pd.DataFrame({"lclid": ["MAC-A", "MAC-B", "MAC-C"], "tarifa": ["ToU", "Std", "Std"],
                        "acorn": ["ACORN-E", "ACORN-Q", "ACORN-Q"], "transformador": ["Z3-T02", "Z3-T07", "Z3-T07"]})
clima = pd.DataFrame({"fecha": ["2013-01-14", "2013-01-15", "2013-01-16"], "temp_max_c": [2.9, 2.4, -0.1]})
tabla("Maestro de hogares (batch, casi no cambia)", hogares)
tabla("Clima diario (batch)", clima)

# Historia: lunes y martes, tal como llegan los CSV (texto con espacios, Null, hora fuera de grilla)
historia = []
base = {"MAC-A": [0.35, 0.40, 0.42, 0.38], "MAC-B": [0.20, 0.22, 0.25, 0.21], "MAC-C": [0.30, 0.33, 0.31, 0.29]}
for dia, ajuste in (("2013-01-14", 1.00), ("2013-01-15", 1.10)):
    for lclid, valores in base.items():
        for hora, v in zip(HORAS, valores):
            historia.append({"lclid": lclid, "tstp": f"{dia} {hora}:00.0000000", "kwh_txt": f" {v * ajuste:.2f} "})
historia.append({"lclid": "MAC-B", "tstp": "2013-01-15 18:13:41.0000000", "kwh_txt": "Null"})

# ---------------------------------------------------------------- batch nocturno (martes)
titulo("1 · MARTES 02:00 · BATCH (silver.py + gold.py): aprende qué es normal")
bronze = pd.DataFrame(historia)
tabla("BRONZE raw/kaggle (tal cual llegó; mostramos 5 de 25 filas)", bronze.tail(5))

silver = bronze.assign(
    ts_utc=pd.to_datetime(bronze.tstp.str[:19]),
    kwh=pd.to_numeric(bronze.kwh_txt.str.strip(), errors="coerce"))
silver["es_nulo"] = silver.kwh.isna()
silver["fuera_de_grilla"] = ~silver.ts_utc.dt.minute.isin([0, 30]) | (silver.ts_utc.dt.second != 0)
silver = silver[["lclid", "ts_utc", "kwh", "es_nulo", "fuera_de_grilla"]]
tabla("SILVER (tipado y con banderas; la fila 18:13:41 queda marcada y no pasa a gold)", silver.tail(5))

gold = silver[~silver.fuera_de_grilla].merge(hogares, on="lclid")
gold["hora"] = gold.ts_utc.dt.strftime("%H:%M")
perfil = (gold.groupby(["lclid", "hora"]).kwh
          .agg(kwh_media="mean", kwh_max_historico="max").round(3).reset_index())
perfil["umbral"] = (perfil.kwh_max_historico * 4).clip(lower=1.5).round(2)   # en el real: max(4×p95, media+8σ, 1.5)
tabla("GOLD → BigQuery lumina_curated.perfil_hogar (REGLAS que usará el streaming)", perfil)

curated = gold[["lclid", "ts_utc", "kwh"]].copy()
print(f"\nlumina_curated.fact_consumo_30min: {len(curated)} filas definitivas (lunes y martes)")

# ---------------------------------------------------------------- streaming en vivo (miércoles)
titulo("2 · MIÉRCOLES EN VIVO · STREAMING (streaming_job.py): cada lectura se evalúa en segundos")
print("   punto 2: el streaming cargó perfil_hogar del batch (reglas, no datos)")
en_vivo = [("18:00", "MAC-A", 0.41), ("18:00", "MAC-B", 0.24), ("18:00", "MAC-C", 0.32),
           ("18:30", "MAC-A", 4.10), ("18:30", "MAC-B", 0.26),                       # MAC-C deja de enviar
           ("19:00", "MAC-A", 0.43), ("19:00", "MAC-B", 0.27),
           ("19:30", "MAC-A", 0.40), ("19:30", "MAC-B", 0.23)]
bronze_streaming, rt_alertas = [], []
ultima = {}
for hora, lclid, kwh in en_vivo:
    ts = pd.Timestamp(f"2013-01-16 {hora}")
    bronze_streaming.append({"lclid": lclid, "ts_utc": ts, "kwh": kwh})            # punto 1: datos a bronze
    ultima[lclid] = ts
    regla = perfil[(perfil.lclid == lclid) & (perfil.hora == hora)].iloc[0]
    estado = "normal"
    if kwh > regla.umbral:
        estado = "🔴 consumo_anomalo"
        rt_alertas.append({"ts_evento": ts, "tipo": "consumo_anomalo", "lclid": lclid,
                           "valor": kwh, "umbral": regla.umbral})
    print(f"   {hora} llega {lclid} {kwh:>5.2f} kWh → umbral {regla.umbral:.2f} → {estado}")
    for medidor, vista in ultima.items():                                            # silencio: 1 h sin datos
        if ts - vista >= pd.Timedelta("1h") and not any(a["lclid"] == medidor and a["tipo"] == "medidor_silencioso"
                                                         for a in rt_alertas):
            rt_alertas.append({"ts_evento": vista, "tipo": "medidor_silencioso", "lclid": medidor,
                               "valor": None, "umbral": None})
            print(f"   {hora} 🔴 medidor_silencioso: {medidor} no envía desde las {vista:%H:%M}")

tabla("lumina_rt.alertas (TABLA, dato provisional)", pd.DataFrame(rt_alertas))
tabla("raw/streaming (BRONZE: copia cruda de cada lectura en vivo, punto 1)", pd.DataFrame(bronze_streaming))

# ---------------------------------------------------------------- vista antes del batch
titulo("3 · MIÉRCOLES 22:00 · VISTA lumina_olap (puntos 3 y 4): historia del batch + hoy del streaming")
hoy = pd.DataFrame(bronze_streaming).assign(fuente="streaming (provisional)")
vista = pd.concat([curated.assign(fuente="batch (definitivo)"), hoy])
resumen = (vista.assign(fecha=vista.ts_utc.dt.date).groupby(["fecha", "fuente"])
           .agg(lecturas=("kwh", "size"), kwh_total=("kwh", "sum")).round(2).reset_index())
tabla("Consumo por día en la vista", resumen)

# ---------------------------------------------------------------- batch consolida (jueves)
titulo("4 · JUEVES 02:00 · BATCH consolida el miércoles: manda el batch")
atrasadas = [{"lclid": "MAC-C", "ts_utc": pd.Timestamp(f"2013-01-16 {h}"), "kwh": v}
             for h, v in (("18:30", 0.34), ("19:00", 0.33), ("19:30", 0.30))]
print("   el concentrador Z3-T07 volvió a las 08:15 y reenvió 3 lecturas de MAC-C que el streaming nunca vio")
miercoles = pd.concat([pd.DataFrame(bronze_streaming), pd.DataFrame(atrasadas)])
curated = pd.concat([curated, miercoles]).drop_duplicates(["lclid", "ts_utc"])
print(f"   lumina_curated.fact_consumo_30min: ahora {len(curated)} filas (el miércoles quedó completo y definitivo)")

vista = curated.assign(fuente="batch (definitivo)")
resumen = (vista.assign(fecha=vista.ts_utc.dt.date).groupby(["fecha", "fuente"])
           .agg(lecturas=("kwh", "size"), kwh_total=("kwh", "sum")).round(2).reset_index())
tabla("Consumo por día en la vista: el miércoles ya sale del batch (12 lecturas, no 9)", resumen)
print("\n   y el batch recalcula perfil_hogar con el miércoles: el streaming lo recarga (punto 2) y sigue el ciclo.")
