"""Lumina · Spark Structured Streaming sobre Kafka (capa de velocidad de la arquitectura Lambda).

Consultas (cada una con su checkpoint en GCS):
  1. anomalias  - lectura vs. perfil del hogar (misma media hora local y tipo de día)
                  -> lumina_rt.alertas (consumo_anomalo, lectura_negativa) + tópico alerts.meter
  2. carga_zona - ventanas de 30 min por zona vs. p95 histórico de la estación (hora punta)
                  -> lumina_rt.carga_zona_30min; picos -> alertas (pico_zona) + tópico alerts.dr
  3. silencio   - estado por medidor con timeout en tiempo de evento
                  -> alertas (medidor_silencioso) + tópico alerts.meter
  4. demanda    - demanda nacional (grid.demand) -> lumina_rt.demanda_red
  5. bronze     - sincronización 1: copia cruda de cada lectura a gs://<raw>/streaming/lecturas
                  (Parquet por fecha, exactly-once); el batch nocturno la consolida en curated

Sincronización 2: perfiles y umbrales vienen del batch (lumina_curated) y se recargan cada
--refresco-horas dentro de los micro-lotes, para que el streaming use lo último que aprendió el batch.

Uso (dentro del contenedor spark, ver infra/ops/docker-compose.yml):
  spark-submit streaming_job.py --proyecto P --checkpoints gs://<curated>/checkpoints/streaming \
      --bronze gs://<raw>/streaming/lecturas
"""
from __future__ import annotations

import argparse
import threading
import time
from typing import Iterator

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

ZONA_LONDRES = "Europe/London"
WATERMARK = "1 hour"                 # tolerancia a lecturas atrasadas (tiempo de evento)
SILENCIO_MS = 2 * 3600 * 1000        # 4 intervalos sin datos -> medidor silencioso
PUNTA_DESDE, PUNTA_HASTA = 17, 20   # hora punta local: solo ahí se emiten señales DR
FACTOR_P95, SIGMAS, KWH_MINIMO = 4.0, 8.0, 1.5   # calibrado: ~0.02 % de lecturas (semana 14-20 ene 2013)

LECTURA = StructType([StructField("lclid", StringType()), StructField("ts_utc", StringType()),
                      StructField("kwh", DoubleType())])
DEMANDA = StructType([StructField("ts_utc", StringType()), StructField("nd_mw", DoubleType()),
                      StructField("eolica_embebida_mw", DoubleType()),
                      StructField("solar_embebida_mw", DoubleType())])


def argumentos() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--proyecto", required=True)
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--checkpoints", required=True)
    p.add_argument("--bronze", default="", help="Destino de la copia cruda (vacío = no se escribe)")
    p.add_argument("--refresco-horas", type=float, default=24.0,
                   help="Cada cuántas horas se recargan perfiles y umbrales del batch")
    p.add_argument("--trigger", default="5 seconds")
    return p.parse_args()


class Contexto:
    """Conexiones y tablas de referencia. Las reglas del batch se recargan cada `refresco-horas`."""

    def __init__(self, spark: SparkSession, args: argparse.Namespace):
        self.spark, self.args = spark, args
        self._lock = threading.Lock()
        self._cargado_en = 0.0
        # dim_hogar se usa también dentro del plan de streaming (carga_zona agrupa por zona);
        # esa copia queda fija hasta reiniciar el job (los hogares casi no cambian).
        self.hogar_stream = F.broadcast(self._leer("dim_hogar").select("lclid", "zona", "transformador").cache())
        self._cargar_reglas()

    def _leer(self, tabla: str) -> DataFrame:
        return (self.spark.read.format("bigquery")
                .option("table", f"{self.args.proyecto}.lumina_curated.{tabla}").load())

    def _cargar_reglas(self) -> None:
        nuevas = {
            "hogar": self._leer("dim_hogar").select("lclid", "zona", "transformador"),
            "fechas": self._leer("dim_fecha").select("fecha", "tipo_dia", "estacion"),
            "perfil": self._leer("perfil_hogar").select(
                "lclid", "tipo_dia", "periodo_local", "kwh_media", "kwh_std", "kwh_p95"),
            "umbral": self._leer("umbral_zona").select("zona", "tipo_dia", "estacion", "periodo_local", "kw_p95"),
        }
        filas = {}
        for nombre, df in nuevas.items():
            df = df.cache()
            filas[nombre] = df.count()          # materializa antes de reemplazar la versión anterior
            anterior = getattr(self, f"_{nombre}", None)
            setattr(self, f"_{nombre}", df)
            if anterior is not None:
                anterior.unpersist()
        self._cargado_en = time.time()
        print(f"[lumina] reglas del batch cargadas: {filas}", flush=True)

    def refrescar(self) -> None:
        """Sincronización 2: recarga perfiles y umbrales si pasó el intervalo (seguro entre consultas)."""
        if time.time() - self._cargado_en < self.args.refresco_horas * 3600:
            return
        with self._lock:
            if time.time() - self._cargado_en >= self.args.refresco_horas * 3600:
                self._cargar_reglas()

    # Las tablas de referencia se usan dentro de foreachBatch, así cada micro-lote ve la versión vigente.
    hogar = property(lambda self: F.broadcast(self._hogar))
    fechas = property(lambda self: F.broadcast(self._fechas))
    perfil = property(lambda self: F.broadcast(self._perfil))
    umbral = property(lambda self: F.broadcast(self._umbral))

    def tabla(self, nombre: str) -> str:
        return f"{self.args.proyecto}.lumina_rt.{nombre}"

    def a_bigquery(self, df: DataFrame, tabla: str) -> None:
        (df.write.format("bigquery").option("writeMethod", "direct")
           .option("table", self.tabla(tabla)).mode("append").save())

    def a_kafka(self, df: DataFrame, topico: str, clave: str) -> None:
        (df.select(F.col(clave).cast("string").alias("key"),
                   F.to_json(F.struct(*df.columns)).alias("value"))
           .write.format("kafka").option("kafka.bootstrap.servers", self.args.bootstrap)
           .option("topic", topico).save())

    def kafka(self, topico: str) -> DataFrame:
        return (self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", self.args.bootstrap)
                .option("subscribe", topico)
                .option("startingOffsets", "latest")
                .option("maxOffsetsPerTrigger", 200_000)
                .load())


def periodo_local(col_ts: str) -> F.Column:
    local = F.from_utc_timestamp(col_ts, ZONA_LONDRES)
    return (F.hour(local) * 2 + F.floor(F.minute(local) / 30) + 1).cast("int")


def lecturas(ctx: Contexto) -> DataFrame:
    return (ctx.kafka("meter.readings")
            .select(F.from_json(F.col("value").cast("string"), LECTURA).alias("v"),
                    F.col("timestamp").alias("ts_kafka"))
            .select("v.*", "ts_kafka")
            .withColumn("ts_utc", F.to_timestamp("ts_utc"))
            .where(F.col("lclid").isNotNull() & F.col("ts_utc").isNotNull())
            .withWatermark("ts_utc", WATERMARK))


def columnas_alerta(df: DataFrame, tipo: str | F.Column, valor: str, umbral: str | F.Column) -> DataFrame:
    return df.select(
        F.col("ts_utc").alias("ts_evento"),
        F.to_date("ts_utc").alias("fecha"),
        (F.lit(tipo) if isinstance(tipo, str) else tipo).alias("tipo"),
        F.col("lclid").cast("string"),
        F.col("zona").cast("long"),
        F.col("transformador").cast("string"),
        F.col(valor).cast("double").alias("valor"),
        (F.col(umbral) if isinstance(umbral, str) else umbral).cast("double").alias("umbral"),
        F.col("ts_kafka").cast("timestamp"),
        F.current_timestamp().alias("ts_procesado"),
    )


# --------------------------------------------------------------------- 1. anomalías
def consulta_anomalias(ctx: Contexto, lect: DataFrame):
    umbral = F.greatest(F.col("kwh_p95") * FACTOR_P95, F.col("kwh_media") + SIGMAS * F.col("kwh_std"),
                        F.lit(KWH_MINIMO))
    base = (lect.withColumn("fecha", F.to_date("ts_utc"))
            .withColumn("periodo_local", periodo_local("ts_utc")))

    def lote(df: DataFrame, batch_id: int) -> None:
        ctx.refrescar()
        if df.isEmpty():
            return
        alertas = columnas_alerta(
            df.join(ctx.fechas, "fecha", "left")
              .join(ctx.hogar, "lclid", "left")
              .join(ctx.perfil, ["lclid", "tipo_dia", "periodo_local"], "left")
              .withColumn("umbral", umbral)
              .where((F.col("kwh") < 0) | (F.col("kwh") > F.col("umbral"))),
            F.when(F.col("kwh") < 0, "lectura_negativa").otherwise("consumo_anomalo"), "kwh", "umbral").cache()
        n = alertas.count()
        if n:
            ctx.a_bigquery(alertas, "alertas")
            ctx.a_kafka(alertas, "alerts.meter", "lclid")
        print(f"[anomalias] lote {batch_id}: {n} alertas", flush=True)
        alertas.unpersist()

    return (base.writeStream.foreachBatch(lote).queryName("anomalias")
            .option("checkpointLocation", f"{ctx.args.checkpoints}/anomalias")
            .trigger(processingTime=ctx.args.trigger).start())


# --------------------------------------------------------------------- 2. carga por zona
def consulta_carga_zona(ctx: Contexto, lect: DataFrame):
    carga = (lect.join(ctx.hogar_stream, "lclid", "left")
             .groupBy(F.window("ts_utc", "30 minutes"), "zona")
             .agg(F.sum("kwh").alias("kwh_total"), F.count("kwh").alias("n_lecturas"),
                  F.max("ts_kafka").alias("ts_kafka")))

    def lote(df: DataFrame, batch_id: int) -> None:
        ctx.refrescar()
        if df.isEmpty():
            return
        res = (df.withColumn("ts_utc", F.col("window.start"))
               .withColumn("fecha", F.to_date("ts_utc"))
               .withColumn("periodo_local", periodo_local("ts_utc"))
               .join(ctx.fechas, "fecha", "left")
               .join(ctx.umbral, ["zona", "tipo_dia", "estacion", "periodo_local"], "left")
               .withColumn("kw_por_medidor", F.col("kwh_total") * 2 / F.col("n_lecturas"))
               .withColumn("es_pico", F.coalesce(
                   (F.col("kw_por_medidor") > F.col("kw_p95"))
                   & F.hour(F.from_utc_timestamp("ts_utc", ZONA_LONDRES)).between(PUNTA_DESDE, PUNTA_HASTA),
                   F.lit(False)))
               .cache())
        ctx.a_bigquery(res.select(
            F.col("ts_utc").alias("ventana_inicio"), "fecha", F.col("zona").cast("long"),
            "kwh_total", F.col("n_lecturas").cast("long"), "kw_por_medidor", "kw_p95", "es_pico",
            F.current_timestamp().alias("ts_procesado")), "carga_zona_30min")
        picos = columnas_alerta(
            res.where("es_pico").withColumn("lclid", F.lit(None).cast("string"))
               .withColumn("transformador", F.lit(None).cast("string")),
            "pico_zona", "kw_por_medidor", "kw_p95")
        n_picos = picos.count()
        if n_picos:
            ctx.a_bigquery(picos, "alertas")
            ctx.a_kafka(picos, "alerts.dr", "zona")   # señal de respuesta a la demanda
        print(f"[carga_zona] lote {batch_id}: {res.count()} ventanas, {n_picos} picos", flush=True)
        res.unpersist()

    return (carga.writeStream.foreachBatch(lote).queryName("carga_zona").outputMode("append")
            .option("checkpointLocation", f"{ctx.args.checkpoints}/carga_zona")
            .trigger(processingTime=ctx.args.trigger).start())


# --------------------------------------------------------------------- 3. medidores silenciosos
SALIDA_SILENCIO = StructType([StructField("lclid", StringType()), StructField("ultima_ms", LongType())])
ESTADO_SILENCIO = StructType([StructField("ultima_ms", LongType())])


def detectar_silencio(clave: tuple, lotes: Iterator[pd.DataFrame], estado: GroupState) -> Iterator[pd.DataFrame]:
    if estado.hasTimedOut:
        (ultima,) = estado.get
        estado.remove()
        yield pd.DataFrame({"lclid": [clave[0]], "ultima_ms": [ultima]})
        return
    ultima = max(int(pdf["ts_ms"].max()) for pdf in lotes)
    if estado.exists:
        ultima = max(ultima, estado.get[0])
    estado.update((ultima,))
    estado.setTimeoutTimestamp(ultima + SILENCIO_MS)


def consulta_silencio(ctx: Contexto, lect: DataFrame):
    silencios = (lect.select("lclid", "ts_utc", (F.col("ts_utc").cast("double") * 1000).cast("long").alias("ts_ms"))
                 .groupBy("lclid")
                 .applyInPandasWithState(detectar_silencio, SALIDA_SILENCIO, ESTADO_SILENCIO,
                                         "append", GroupStateTimeout.EventTimeTimeout))

    def lote(df: DataFrame, batch_id: int) -> None:
        if df.isEmpty():
            return
        res = columnas_alerta(
            df.withColumn("ts_utc", (F.col("ultima_ms") / 1000).cast("timestamp"))
              .withColumn("horas", F.lit(SILENCIO_MS / 3_600_000))
              .withColumn("ts_kafka", F.lit(None).cast("timestamp"))
              .join(ctx.hogar, "lclid", "left"),
            "medidor_silencioso", "horas", F.lit(SILENCIO_MS / 3_600_000)).cache()
        ctx.a_bigquery(res, "alertas")
        ctx.a_kafka(res, "alerts.meter", "lclid")
        print(f"[silencio] lote {batch_id}: {res.count()} medidores silenciosos", flush=True)
        res.unpersist()

    return (silencios.writeStream.foreachBatch(lote).queryName("silencio").outputMode("append")
            .option("checkpointLocation", f"{ctx.args.checkpoints}/silencio")
            .trigger(processingTime=ctx.args.trigger).start())


# --------------------------------------------------------------------- 4. demanda nacional
def consulta_demanda(ctx: Contexto):
    demanda = (ctx.kafka("grid.demand")
               .select(F.from_json(F.col("value").cast("string"), DEMANDA).alias("v")).select("v.*")
               .withColumn("ts_utc", F.to_timestamp("ts_utc"))
               .withColumn("fecha", F.to_date("ts_utc"))
               .withColumn("ts_procesado", F.current_timestamp()))

    def lote(df: DataFrame, batch_id: int) -> None:
        if not df.isEmpty():
            ctx.a_bigquery(df.select("ts_utc", "fecha", "nd_mw", "eolica_embebida_mw",
                                     "solar_embebida_mw", "ts_procesado"), "demanda_red")

    return (demanda.writeStream.foreachBatch(lote).queryName("demanda")
            .option("checkpointLocation", f"{ctx.args.checkpoints}/demanda")
            .trigger(processingTime=ctx.args.trigger).start())


# --------------------------------------------------------------------- 5. bronze (sincronización 1)
def consulta_bronze(ctx: Contexto):
    """Copia cruda de cada lectura tal como llegó a Kafka; el file sink de Spark es exactly-once."""
    crudo = (ctx.kafka("meter.readings")
             .select(F.col("value").cast("string").alias("valor_json"),
                     F.col("topic").alias("topico"), F.col("partition").alias("particion"),
                     F.col("offset"), F.col("timestamp").alias("ts_kafka"))
             .withColumn("v", F.from_json("valor_json", LECTURA))
             .select(F.col("v.lclid").alias("lclid"), F.col("v.ts_utc").alias("ts_utc"),
                     F.col("v.kwh").alias("kwh"), "valor_json", "topico", "particion", "offset", "ts_kafka")
             .withColumn("fecha", F.to_date(F.substring("ts_utc", 1, 10))))
    return (crudo.writeStream.format("parquet").queryName("bronze")
            .option("path", ctx.args.bronze)
            .option("checkpointLocation", f"{ctx.args.checkpoints}/bronze")
            .partitionBy("fecha")
            .trigger(processingTime="1 minute")     # archivos más grandes y menos numerosos
            .start())


def main() -> None:
    args = argumentos()
    spark = (SparkSession.builder.appName("lumina-streaming")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", 8)
             .config("spark.sql.streaming.stateStore.providerClass",
                     "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    ctx = Contexto(spark, args)
    lect = lecturas(ctx)
    consultas = [consulta_anomalias(ctx, lect), consulta_carga_zona(ctx, lect),
                 consulta_silencio(ctx, lect), consulta_demanda(ctx)]
    if args.bronze:
        consultas.append(consulta_bronze(ctx))
    print(f"[lumina] {len(consultas)} consultas activas: {[q.name for q in consultas]}", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
