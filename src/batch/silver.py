"""Bronze (CSV de Kaggle) -> silver (Parquet tipado, en UTC, con banderas de calidad).

Uso:
  spark-submit silver.py --entrada gs://<p>-lumina-raw/kaggle --salida gs://<p>-lumina-curated/silver
  (--muestra-bloques 2 para una prueba barata)
Reglas de limpieza documentadas en docs/02_modelo_datos.md.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from lumina_common import ZONA_LONDRES, escribir, parse_args, spark_session

NUMERO = r"^-?\d+(\.\d+)?([eE]-?\d+)?$"


def csv(spark: SparkSession, ruta: str, **opciones) -> DataFrame:
    return spark.read.option("header", True).options(**opciones).csv(ruta)


def lecturas_30min(spark: SparkSession, entrada: str, muestra: int) -> DataFrame:
    base = f"{entrada}/smart_meters_london/halfhourly_dataset/halfhourly_dataset"
    rutas = [f"{base}/block_{i}.csv" for i in range(muestra)] if muestra else [f"{base}/block_*.csv"]
    esquema = StructType([StructField(c, StringType()) for c in ("lclid", "tstp", "kwh_txt")])
    df = spark.read.option("header", True).schema(esquema).csv(rutas)

    kwh_txt = F.trim("kwh_txt")
    ts = F.to_timestamp(F.substring("tstp", 1, 19), "yyyy-MM-dd HH:mm:ss")
    df = (
        df.withColumn("ts_utc", ts)
        .withColumn("kwh", F.when(kwh_txt.rlike(NUMERO), kwh_txt.cast("double")))
        .withColumn("es_nulo", F.col("kwh").isNull())
        .withColumn("fuera_de_grilla",
                    ~(F.minute("ts_utc").isin(0, 30) & (F.second("ts_utc") == 0)))
        .withColumn("bloque", F.regexp_extract(F.input_file_name(), r"block_(\d+)", 1).cast("int"))
        .where(F.col("lclid").isNotNull() & F.col("ts_utc").isNotNull())
        .dropDuplicates(["lclid", "ts_utc"])
        .withColumn("fecha", F.to_date("ts_utc"))
        .withColumn("anio_mes", F.date_format("ts_utc", "yyyy-MM"))
        .select("lclid", "ts_utc", "fecha", "kwh", "es_nulo", "fuera_de_grilla", "bloque", "anio_mes")
    )
    return df


def hogares(spark: SparkSession, entrada: str) -> DataFrame:
    df = csv(spark, f"{entrada}/smart_meters_london/informations_households.csv")
    desconocido = F.col("Acorn").isin("ACORN-", "ACORN-U") | F.col("Acorn").isNull()
    return df.select(
        F.col("LCLid").alias("lclid"),
        F.col("stdorToU").alias("tarifa"),
        F.when(desconocido, "DESCONOCIDO").otherwise(F.col("Acorn")).alias("acorn"),
        F.when(desconocido, "Desconocido").otherwise(F.col("Acorn_grouped")).alias("acorn_grupo"),
        F.regexp_extract("file", r"block_(\d+)", 1).cast("int").alias("bloque"),
    )


def acorn_atributos(spark: SparkSession, entrada: str) -> DataFrame:
    df = csv(spark, f"{entrada}/smart_meters_london/acorn_details.csv", encoding="ISO-8859-1")
    acorns = [c for c in df.columns if c.startswith("ACORN-")]
    pares = ", ".join(f"'{a}', `{a}`" for a in acorns)
    return df.selectExpr(
        "`MAIN CATEGORIES` as categoria_principal",
        "CATEGORIES as categoria",
        "REFERENCE as referencia",
        f"stack({len(acorns)}, {pares}) as (acorn, indice_txt)",
    ).withColumn("indice", F.col("indice_txt").cast("double")).drop("indice_txt")


def feriados(spark: SparkSession, entrada: str) -> DataFrame:
    df = csv(spark, f"{entrada}/smart_meters_london/uk_bank_holidays.csv")
    return df.select(
        F.to_date(F.col("Bank holidays")).alias("fecha"),
        F.col("Type").alias("nombre_feriado"),
    ).dropDuplicates(["fecha"])


def clima_hora(spark: SparkSession, entrada: str) -> DataFrame:
    df = csv(spark, f"{entrada}/smart_meters_london/weather_hourly_darksky.csv", inferSchema=True)
    return df.select(
        F.to_timestamp("time").alias("ts_utc"),
        F.col("temperature").alias("temperatura_c"),
        F.col("apparentTemperature").alias("sensacion_termica_c"),
        F.col("dewPoint").alias("punto_rocio_c"),
        F.col("humidity").alias("humedad"),
        F.col("windSpeed").alias("viento_ms"),
        F.col("windBearing").alias("viento_dir"),
        F.col("pressure").alias("presion_hpa"),
        F.col("visibility").alias("visibilidad_km"),
        F.col("precipType").alias("tipo_precipitacion"),
        F.col("icon").alias("icono"),
        F.col("summary").alias("resumen"),
    ).withColumn("fecha", F.to_date("ts_utc")).dropDuplicates(["ts_utc"])


def clima_dia(spark: SparkSession, entrada: str) -> DataFrame:
    df = csv(spark, f"{entrada}/smart_meters_london/weather_daily_darksky.csv", inferSchema=True)
    return df.select(
        F.to_date("time").alias("fecha"),
        F.col("temperatureMax").alias("temp_max_c"),
        F.col("temperatureMin").alias("temp_min_c"),
        F.col("apparentTemperatureMax").alias("sensacion_max_c"),
        F.col("apparentTemperatureMin").alias("sensacion_min_c"),
        F.col("humidity").alias("humedad"),
        F.col("cloudCover").alias("nubosidad"),
        F.col("windSpeed").alias("viento_ms"),
        F.col("pressure").alias("presion_hpa"),
        F.col("uvIndex").alias("indice_uv"),
        F.col("precipType").alias("tipo_precipitacion"),
        F.col("icon").alias("icono"),
        F.to_timestamp("sunriseTime").alias("amanecer_utc"),
        F.to_timestamp("sunsetTime").alias("atardecer_utc"),
    ).dropDuplicates(["fecha"])


def demanda_nacional(spark: SparkSession, entrada: str) -> DataFrame:
    df = csv(spark, f"{entrada}/uk_national_demand/historic_demand_2009_2024.csv", inferSchema=True)
    fecha = F.to_date("settlement_date")
    # Periodo de liquidación en hora local de Londres (46/50 periodos en cambio de hora).
    inicio_dia_utc = F.to_utc_timestamp(fecha.cast("timestamp"), ZONA_LONDRES)
    ts_utc = (F.unix_timestamp(inicio_dia_utc) + (F.col("settlement_period") - 1) * 1800).cast("timestamp")
    return df.select(
        ts_utc.alias("ts_utc"),
        fecha.alias("fecha_liquidacion"),
        F.col("settlement_period").cast("int").alias("periodo_liquidacion"),
        F.col("nd").cast("double").alias("nd_mw"),
        F.col("tsd").cast("double").alias("tsd_mw"),
        F.col("england_wales_demand").cast("double").alias("demanda_ew_mw"),
        F.col("embedded_wind_generation").cast("double").alias("eolica_embebida_mw"),
        F.col("embedded_wind_capacity").cast("double").alias("eolica_capacidad_mw"),
        F.col("embedded_solar_generation").cast("double").alias("solar_embebida_mw"),
        F.col("embedded_solar_capacity").cast("double").alias("solar_capacidad_mw"),
        F.col("is_holiday").cast("int").cast("boolean").alias("es_feriado"),
    ).withColumn("fecha", F.to_date("ts_utc")).dropDuplicates(["ts_utc"])


def main() -> None:
    args = parse_args("Lumina · bronze -> silver")
    spark = spark_session("lumina-silver")
    e, s = args.entrada.rstrip("/"), args.salida.rstrip("/")

    escribir(hogares(spark, e), f"{s}/hogares", archivos=1)
    escribir(acorn_atributos(spark, e), f"{s}/acorn_atributos", archivos=1)
    escribir(feriados(spark, e), f"{s}/feriados", archivos=1)
    escribir(clima_hora(spark, e), f"{s}/clima_hora", archivos=1)
    escribir(clima_dia(spark, e), f"{s}/clima_dia", archivos=1)
    escribir(demanda_nacional(spark, e), f"{s}/demanda_nacional_30min", archivos=1)
    # ~28 meses -> un archivo por mes (60-120 MB c/u con la data completa)
    escribir(lecturas_30min(spark, e, args.muestra_bloques), f"{s}/lecturas_30min",
             particion="anio_mes", archivos=32)
    spark.stop()


if __name__ == "__main__":
    main()
