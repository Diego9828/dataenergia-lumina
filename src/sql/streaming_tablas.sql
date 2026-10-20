-- Fase 3 · Tablas de apoyo para el streaming. Uso: infra/07_vm_ops.sh tablas
-- ${P} se reemplaza por el ID del proyecto.

-- Umbral de pico por zona, estación, tipo de día y media hora local, normalizado por medidor
-- (la cantidad de medidores activos cambia en el tiempo). Por estación: un p95 de todo el año
-- marcaba el 63 % de las ventanas de una semana fría; el de invierno, en hora punta, el 7 %.
CREATE OR REPLACE TABLE `${P}.lumina_curated.umbral_zona` AS
WITH carga AS (
  SELECT
    z.zona,
    f.tipo_dia,
    f.estacion,
    EXTRACT(HOUR FROM DATETIME(z.ts_utc, 'Europe/London')) * 2
      + DIV(EXTRACT(MINUTE FROM DATETIME(z.ts_utc, 'Europe/London')), 30) + 1 AS periodo_local,
    z.kw_medio / z.n_medidores AS kw_por_medidor
  FROM `${P}.lumina_curated.fact_carga_zona_30min` z
  JOIN `${P}.lumina_curated.dim_fecha` f USING (fecha)
  WHERE z.n_medidores >= 100
)
SELECT
  zona, tipo_dia, estacion, periodo_local,
  ROUND(APPROX_QUANTILES(kw_por_medidor, 100)[OFFSET(50)], 4) AS kw_p50,
  ROUND(APPROX_QUANTILES(kw_por_medidor, 100)[OFFSET(95)], 4) AS kw_p95,
  COUNT(*) AS n_observaciones
FROM carga
GROUP BY zona, tipo_dia, estacion, periodo_local;

-- Salidas del streaming (dataset lumina_rt).
-- Se particionan por fecha de PROCESAMIENTO: la Storage Write API solo acepta filas cuya
-- partición esté cerca de la fecha actual, y el replay emite fechas de 2013.
CREATE TABLE IF NOT EXISTS `${P}.lumina_rt.alertas` (
  ts_evento TIMESTAMP,
  fecha DATE,
  tipo STRING,                   -- consumo_anomalo | lectura_negativa | medidor_silencioso | pico_zona
  lclid STRING,
  zona INT64,
  transformador STRING,
  valor FLOAT64,
  umbral FLOAT64,
  ts_kafka TIMESTAMP,            -- llegada a Kafka (para medir latencia)
  ts_procesado TIMESTAMP
)
PARTITION BY DATE(ts_procesado)
CLUSTER BY tipo, zona;

CREATE TABLE IF NOT EXISTS `${P}.lumina_rt.carga_zona_30min` (
  ventana_inicio TIMESTAMP,
  fecha DATE,
  zona INT64,
  kwh_total FLOAT64,
  n_lecturas INT64,
  kw_por_medidor FLOAT64,
  kw_p95 FLOAT64,
  es_pico BOOL,
  ts_procesado TIMESTAMP
)
PARTITION BY DATE(ts_procesado)
CLUSTER BY zona;

CREATE TABLE IF NOT EXISTS `${P}.lumina_rt.demanda_red` (
  ts_utc TIMESTAMP,
  fecha DATE,
  nd_mw FLOAT64,
  eolica_embebida_mw FLOAT64,
  solar_embebida_mw FLOAT64,
  ts_procesado TIMESTAMP
)
PARTITION BY DATE(ts_procesado);
