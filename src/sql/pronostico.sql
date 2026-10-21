-- Fase 4 · Pronóstico de demanda del día siguiente (vertical II · planes diarios) con BigQuery ML.
-- Dos modelos BOOSTED_TREE_REGRESSOR (se facturan a la tarifa normal de consultas):
--   modelo_zona_d1   carga de cada media hora de mañana por zona (kW por medidor)
--   modelo_hogar_d1  kWh totales de mañana por hogar
-- Solo usa lo conocido al cierre del día D (rezagos de 1 y 7 días, promedio de 7 días) y el clima de
-- D+1, con el clima observado como "pronóstico perfecto" (supuesto declarado).
-- Entrenamiento hasta 2013-12-31; prueba fuera de muestra: 2014-01-01 a 2014-02-27.
-- Uso: bash infra/10_pronostico.sh      ${P} = proyecto

-- ------------------------------------------------------------------ zona · media hora
CREATE OR REPLACE TABLE `${P}.lumina_ml.features_zona_30min` AS
WITH base AS (
  SELECT zona, ts_utc, fecha, kw_medio / n_medidores AS y
  FROM `${P}.lumina_curated.fact_carga_zona_30min`
  WHERE n_medidores >= 100
),
rezagos AS (
  SELECT
    *,
    -- misma media hora (UTC) en días anteriores; RANGE sobre días tolera huecos
    AVG(y) OVER (PARTITION BY zona, EXTRACT(TIME FROM ts_utc) ORDER BY UNIX_DATE(fecha)
                 RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING) AS lag_1d,
    AVG(y) OVER (PARTITION BY zona, EXTRACT(TIME FROM ts_utc) ORDER BY UNIX_DATE(fecha)
                 RANGE BETWEEN 7 PRECEDING AND 7 PRECEDING) AS lag_7d,
    AVG(y) OVER (PARTITION BY zona, EXTRACT(TIME FROM ts_utc) ORDER BY UNIX_DATE(fecha)
                 RANGE BETWEEN 7 PRECEDING AND 1 PRECEDING) AS media_7d
  FROM base
)
SELECT
  r.zona, r.ts_utc, r.fecha, r.y, r.lag_1d, r.lag_7d, r.media_7d,
  CAST(r.zona AS STRING) AS zona_txt,
  EXTRACT(HOUR FROM DATETIME(r.ts_utc, 'Europe/London')) * 2
    + DIV(EXTRACT(MINUTE FROM DATETIME(r.ts_utc, 'Europe/London')), 30) + 1 AS periodo_local,
  CAST(f.dia_semana AS STRING) AS dia_semana, f.tipo_dia, f.estacion, f.es_feriado, f.mes,
  ch.temperatura_c, ch.humedad, cd.temp_max_c, cd.temp_min_c,
  IF(r.fecha < '2014-01-01', 'entrenamiento', 'prueba') AS conjunto
FROM rezagos r
JOIN `${P}.lumina_curated.dim_fecha` f USING (fecha)
LEFT JOIN `${P}.lumina_curated.fact_clima_hora` ch ON ch.ts_utc = TIMESTAMP_TRUNC(r.ts_utc, HOUR)
LEFT JOIN `${P}.lumina_curated.fact_clima_dia` cd ON cd.fecha = r.fecha
WHERE r.lag_1d IS NOT NULL AND r.lag_7d IS NOT NULL AND r.media_7d IS NOT NULL;

CREATE OR REPLACE MODEL `${P}.lumina_ml.modelo_zona_d1`
OPTIONS (model_type = 'BOOSTED_TREE_REGRESSOR', input_label_cols = ['y'],
         max_iterations = 60, early_stop = TRUE, subsample = 0.8,
         data_split_method = 'RANDOM', data_split_eval_fraction = 0.1) AS
SELECT y, zona_txt, periodo_local, dia_semana, tipo_dia, estacion, es_feriado, mes,
       lag_1d, lag_7d, media_7d, temperatura_c, humedad, temp_max_c, temp_min_c
FROM `${P}.lumina_ml.features_zona_30min`
WHERE conjunto = 'entrenamiento';

CREATE OR REPLACE TABLE `${P}.lumina_ml.pronostico_zona_d1` AS
SELECT zona, ts_utc, fecha, periodo_local, y AS real_kw_por_medidor,
       ROUND(predicted_y, 4) AS pronostico_kw_por_medidor, lag_1d AS ingenuo_ayer
FROM ML.PREDICT(MODEL `${P}.lumina_ml.modelo_zona_d1`,
                (SELECT * FROM `${P}.lumina_ml.features_zona_30min` WHERE conjunto = 'prueba'));

-- ------------------------------------------------------------------ hogar · día
CREATE OR REPLACE TABLE `${P}.lumina_ml.features_hogar_diario` AS
WITH base AS (
  SELECT lclid, fecha, kwh_total AS y
  FROM `${P}.lumina_curated.fact_consumo_diario`
  WHERE cobertura >= 0.9
),
rezagos AS (
  SELECT
    *,
    AVG(y) OVER (PARTITION BY lclid ORDER BY UNIX_DATE(fecha) RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING) AS lag_1d,
    AVG(y) OVER (PARTITION BY lclid ORDER BY UNIX_DATE(fecha) RANGE BETWEEN 7 PRECEDING AND 7 PRECEDING) AS lag_7d,
    AVG(y) OVER (PARTITION BY lclid ORDER BY UNIX_DATE(fecha) RANGE BETWEEN 7 PRECEDING AND 1 PRECEDING) AS media_7d,
    AVG(y) OVER (PARTITION BY lclid ORDER BY UNIX_DATE(fecha) RANGE BETWEEN 28 PRECEDING AND 1 PRECEDING) AS media_28d
  FROM base
)
SELECT
  r.lclid, r.fecha, r.y, r.lag_1d, r.lag_7d, r.media_7d, r.media_28d,
  h.acorn_grupo, h.tarifa,
  CAST(f.dia_semana AS STRING) AS dia_semana, f.tipo_dia, f.estacion, f.es_feriado, f.mes,
  cd.temp_max_c, cd.temp_min_c, cd.humedad,
  IF(r.fecha < '2014-01-01', 'entrenamiento', 'prueba') AS conjunto
FROM rezagos r
JOIN `${P}.lumina_curated.dim_hogar` h USING (lclid)
JOIN `${P}.lumina_curated.dim_fecha` f USING (fecha)
LEFT JOIN `${P}.lumina_curated.fact_clima_dia` cd ON cd.fecha = r.fecha
WHERE r.lag_1d IS NOT NULL AND r.lag_7d IS NOT NULL AND r.media_7d IS NOT NULL;

CREATE OR REPLACE MODEL `${P}.lumina_ml.modelo_hogar_d1`
OPTIONS (model_type = 'BOOSTED_TREE_REGRESSOR', input_label_cols = ['y'],
         max_iterations = 60, early_stop = TRUE, subsample = 0.8,
         data_split_method = 'RANDOM', data_split_eval_fraction = 0.1) AS
SELECT y, acorn_grupo, tarifa, dia_semana, tipo_dia, estacion, es_feriado, mes,
       lag_1d, lag_7d, media_7d, media_28d, temp_max_c, temp_min_c, humedad
FROM `${P}.lumina_ml.features_hogar_diario`
WHERE conjunto = 'entrenamiento';

CREATE OR REPLACE TABLE `${P}.lumina_ml.pronostico_hogar_d1` AS
SELECT lclid, fecha, y AS real_kwh, ROUND(predicted_y, 3) AS pronostico_kwh, lag_1d AS ingenuo_ayer
FROM ML.PREDICT(MODEL `${P}.lumina_ml.modelo_hogar_d1`,
                (SELECT * FROM `${P}.lumina_ml.features_hogar_diario` WHERE conjunto = 'prueba'));

-- ------------------------------------------------------------------ métricas fuera de muestra
CREATE OR REPLACE TABLE `${P}.lumina_ml.metricas_pronostico` AS
SELECT 'zona · media hora' AS modelo, 'boosted tree' AS metodo, COUNT(*) AS n,
       ROUND(AVG(ABS(pronostico_kw_por_medidor - real_kw_por_medidor) / real_kw_por_medidor) * 100, 2) AS mape_pct,
       ROUND(SUM(ABS(pronostico_kw_por_medidor - real_kw_por_medidor)) / SUM(real_kw_por_medidor) * 100, 2) AS wape_pct
FROM `${P}.lumina_ml.pronostico_zona_d1`
UNION ALL
SELECT 'zona · media hora', 'ingenuo (igual que ayer)', COUNT(*),
       ROUND(AVG(ABS(ingenuo_ayer - real_kw_por_medidor) / real_kw_por_medidor) * 100, 2),
       ROUND(SUM(ABS(ingenuo_ayer - real_kw_por_medidor)) / SUM(real_kw_por_medidor) * 100, 2)
FROM `${P}.lumina_ml.pronostico_zona_d1`
UNION ALL
SELECT 'hogar · día', 'boosted tree', COUNT(*),
       ROUND(AVG(SAFE_DIVIDE(ABS(pronostico_kwh - real_kwh), real_kwh)) * 100, 2),
       ROUND(SUM(ABS(pronostico_kwh - real_kwh)) / SUM(real_kwh) * 100, 2)
FROM `${P}.lumina_ml.pronostico_hogar_d1`
UNION ALL
SELECT 'hogar · día', 'ingenuo (igual que ayer)', COUNT(*),
       ROUND(AVG(SAFE_DIVIDE(ABS(ingenuo_ayer - real_kwh), real_kwh)) * 100, 2),
       ROUND(SUM(ABS(ingenuo_ayer - real_kwh)) / SUM(real_kwh) * 100, 2)
FROM `${P}.lumina_ml.pronostico_hogar_d1`;
