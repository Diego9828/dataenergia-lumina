"""Silver -> gold: modelo estrella, agregados, perfiles por hogar y calidad por medidor.

Uso:
  spark-submit gold.py --entrada gs://<p>-lumina-curated/silver --salida gs://<p>-lumina-curated/gold
Modelo documentado en docs/02_modelo_datos.md.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lumina_common import ZONA_LONDRES, escribir, parse_args, spark_session

N_ZONAS = 8
TRANSFORMADORES_POR_ZONA = 14   # ~50 medidores por transformador
PUNTA_DESDE, PUNTA_HASTA = 17, 20   # 17:00-20:59 hora de Londres
INTERVALO_S = 1800


def dim_hogar(hogares: DataFrame) -> DataFrame:
    zona = (F.pmod(F.xxhash64("lclid"), F.lit(N_ZONAS)) + 1).cast("int")
    trafo = (F.pmod(F.xxhash64("lclid", F.lit("trafo")), F.lit(TRANSFORMADORES_POR_ZONA)) + 1).cast("int")
    return (
        hogares.withColumn("zona", zona)
        .withColumn("transformador",
                    F.format_string("Z%d-T%02d", F.col("zona"), trafo))
    )


def dim_fecha(spark: SparkSession, feriados: DataFrame) -> DataFrame:
    fechas = spark.sql(
        "SELECT explode(sequence(DATE'2011-11-01', DATE'2014-03-31', INTERVAL 1 DAY)) AS fecha"
    )
    mes = F.month("fecha")
    df = (
        fechas.join(feriados, "fecha", "left")
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", mes)
        .withColumn("anio_mes", F.date_format("fecha", "yyyy-MM"))
        .withColumn("dia_semana", F.dayofweek("fecha"))          # 1 = domingo
        .withColumn("nombre_dia", F.date_format("fecha", "EEEE"))
        .withColumn("es_fin_de_semana", F.dayofweek("fecha").isin(1, 7))
        .withColumn("es_feriado", F.col("nombre_feriado").isNotNull())
        .withColumn("tipo_dia", F.when(F.col("es_fin_de_semana") | F.col("es_feriado"), "no_laborable")
                    .otherwise("laborable"))
        .withColumn("estacion", F.when(mes.isin(12, 1, 2), "invierno")
                    .when(mes.isin(3, 4, 5), "primavera")
                    .when(mes.isin(6, 7, 8), "verano").otherwise("otono"))
    )
    return df


def fact_consumo_30min(lecturas: DataFrame) -> DataFrame:
    local = F.from_utc_timestamp("ts_utc", ZONA_LONDRES)
    return (
        lecturas.where(~F.col("fuera_de_grilla"))
        .withColumn("hora_local", F.hour(local))
        .withColumn("periodo_local", (F.hour(local) * 2 + F.floor(F.minute(local) / 30) + 1).cast("int"))
        .withColumn("es_punta", F.col("hora_local").between(PUNTA_DESDE, PUNTA_HASTA))
        .select("lclid", "ts_utc", "fecha", "periodo_local", "hora_local", "kwh", "es_nulo",
                "es_punta", "anio_mes")
    )


def fact_consumo_diario(consumo: DataFrame) -> DataFrame:
    return consumo.groupBy("lclid", "fecha").agg(
        F.round(F.sum("kwh"), 4).alias("kwh_total"),
        F.round(F.sum(F.when(F.col("es_punta"), F.col("kwh"))), 4).alias("kwh_punta"),
        F.max("kwh").alias("kwh_max"),
        F.count("kwh").alias("n_lecturas"),
        F.sum(F.col("es_nulo").cast("int")).alias("n_nulos"),
        F.sum((F.col("kwh") == 0).cast("int")).alias("n_ceros"),
    ).withColumn("cobertura", F.round(F.col("n_lecturas") / 48, 4))


def fact_carga_transformador(consumo: DataFrame, hogares: DataFrame) -> DataFrame:
    return (
        consumo.join(F.broadcast(hogares.select("lclid", "zona", "transformador")), "lclid")
        .groupBy("zona", "transformador", "ts_utc", "fecha")
        .agg(F.round(F.sum("kwh"), 4).alias("kwh_total"),
             F.count("kwh").alias("n_medidores"))
        .withColumn("kw_medio", F.round(F.col("kwh_total") * 2, 4))   # kWh en 30 min -> kW
    )


def fact_carga_zona(carga_trafo: DataFrame) -> DataFrame:
    return (
        carga_trafo.groupBy("zona", "ts_utc", "fecha")
        .agg(F.round(F.sum("kwh_total"), 4).alias("kwh_total"),
             F.sum("n_medidores").alias("n_medidores"))
        .withColumn("kw_medio", F.round(F.col("kwh_total") * 2, 4))
    )


def perfil_hogar(consumo: DataFrame, fechas: DataFrame) -> DataFrame:
    """Consumo esperado por hogar, tipo de día y media hora local: base de las anomalías en streaming."""
    return (
        consumo.where(F.col("kwh").isNotNull())
        .join(F.broadcast(fechas.select("fecha", "tipo_dia")), "fecha")
        .groupBy("lclid", "tipo_dia", "periodo_local")
        .agg(F.round(F.avg("kwh"), 4).alias("kwh_media"),
             F.round(F.stddev("kwh"), 4).alias("kwh_std"),
             F.percentile_approx("kwh", 0.5).alias("kwh_p50"),
             F.percentile_approx("kwh", 0.95).alias("kwh_p95"),
             F.count("kwh").alias("n_observaciones"))
    )


def dq_medidor(lecturas: DataFrame) -> DataFrame:
    w = Window.partitionBy("lclid").orderBy("ts_utc")
    validas = lecturas.where(~F.col("fuera_de_grilla"))
    salto = (F.unix_timestamp("ts_utc") - F.unix_timestamp(F.lag("ts_utc").over(w))) / INTERVALO_S
    # Rachas de ceros (gaps-and-islands): cada lectura distinta de cero abre un grupo nuevo.
    grupo_cero = F.sum(F.when(F.col("kwh") == 0, 0).otherwise(1)).over(w)
    detalle = validas.withColumn("salto", salto).withColumn("grupo_cero", grupo_cero)

    rachas = (
        detalle.where(F.col("kwh") == 0)
        .groupBy("lclid", "grupo_cero").count()
        .groupBy("lclid").agg(F.max("count").alias("max_racha_ceros"))
    )
    resumen = detalle.groupBy("lclid").agg(
        F.min("ts_utc").alias("primera_lectura"),
        F.max("ts_utc").alias("ultima_lectura"),
        F.count("*").alias("n_recibidas"),
        F.count("kwh").alias("n_validas"),
        F.sum(F.col("es_nulo").cast("int")).alias("n_nulos"),
        F.sum((F.col("kwh") == 0).cast("int")).alias("n_ceros"),
        F.sum((F.col("kwh") < 0).cast("int")).alias("n_negativos"),
        F.sum((F.col("salto") > 1).cast("int")).alias("n_huecos"),
        F.max(F.col("salto") - 1).cast("int").alias("max_hueco_intervalos"),
    )
    fuera = (lecturas.where(F.col("fuera_de_grilla")).groupBy("lclid")
             .agg(F.count("*").alias("n_fuera_de_grilla")))

    esperadas = ((F.unix_timestamp("ultima_lectura") - F.unix_timestamp("primera_lectura"))
                 / INTERVALO_S + 1)
    df = (
        resumen.join(rachas, "lclid", "left").join(fuera, "lclid", "left")
        .fillna(0, ["max_racha_ceros", "n_fuera_de_grilla", "max_hueco_intervalos"])
        .withColumn("n_esperadas", esperadas.cast("long"))
        .withColumn("cobertura_pct", F.round(F.col("n_validas") / F.col("n_esperadas") * 100, 2))
    )
    return df.withColumn(
        "diagnostico",
        F.when(F.col("n_validas") == 0, "sin_datos")
        .when(F.col("max_racha_ceros") >= 48, "ceros_prolongados")
        .when(F.col("cobertura_pct") < 90, "cobertura_baja")
        .when(F.col("n_huecos") > 10, "huecos_frecuentes")
        .otherwise("ok"),
    )


def main() -> None:
    args = parse_args("Lumina · silver -> gold")
    spark = spark_session("lumina-gold")
    e, s = args.entrada.rstrip("/"), args.salida.rstrip("/")
    leer = lambda nombre: spark.read.parquet(f"{e}/{nombre}")  # noqa: E731

    hogares = dim_hogar(leer("hogares")).cache()
    fechas = dim_fecha(spark, leer("feriados")).cache()
    escribir(hogares, f"{s}/dim_hogar", archivos=1)
    escribir(fechas, f"{s}/dim_fecha", archivos=1)
    escribir(hogares.select("acorn", "acorn_grupo").distinct(), f"{s}/dim_acorn", archivos=1)
    escribir(leer("acorn_atributos"), f"{s}/acorn_atributos", archivos=1)
    escribir(leer("clima_hora"), f"{s}/fact_clima_hora", archivos=1)
    escribir(leer("clima_dia"), f"{s}/fact_clima_dia", archivos=1)
    escribir(leer("demanda_nacional_30min"), f"{s}/fact_demanda_nacional_30min", archivos=1)

    lecturas = leer("lecturas_30min")
    consumo = fact_consumo_30min(lecturas)
    escribir(consumo, f"{s}/fact_consumo_30min", particion="anio_mes", archivos=32)

    consumo = spark.read.parquet(f"{s}/fact_consumo_30min")   # relee lo ya escrito (evita recomputar)
    escribir(fact_consumo_diario(consumo), f"{s}/fact_consumo_diario", archivos=4)
    carga_trafo = fact_carga_transformador(consumo, hogares)
    escribir(carga_trafo, f"{s}/fact_carga_transformador_30min", archivos=4)
    escribir(fact_carga_zona(spark.read.parquet(f"{s}/fact_carga_transformador_30min")),
             f"{s}/fact_carga_zona_30min", archivos=1)
    escribir(perfil_hogar(consumo, fechas), f"{s}/perfil_hogar", archivos=2)
    escribir(dq_medidor(lecturas), f"{s}/dq_medidor", archivos=1)
    spark.stop()


if __name__ == "__main__":
    main()
