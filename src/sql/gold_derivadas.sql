-- Tablas derivadas en BigQuery a partir de gold. Las usan infra/06_cargar_bigquery.sh y el DAG
-- lumina_batch_diario. ${P} = proyecto, ${DS} = dataset destino (lumina_curated o lumina_muestra).

-- Cruce con el warehouse público NOAA GSOD (Heathrow, estación 037720), °F -> °C.
CREATE OR REPLACE TABLE `${P}.${DS}.clima_noaa_heathrow` AS
SELECT
  DATE(CAST(year AS INT64), CAST(mo AS INT64), CAST(da AS INT64)) AS fecha,
  ROUND((NULLIF(temp, 9999.9) - 32) * 5 / 9, 2) AS temp_media_c,
  ROUND((NULLIF(max, 9999.9) - 32) * 5 / 9, 2) AS temp_max_c,
  ROUND((NULLIF(min, 9999.9) - 32) * 5 / 9, 2) AS temp_min_c,
  ROUND(NULLIF(prcp, 99.99) * 25.4, 2) AS precipitacion_mm,
  ROUND(SAFE_CAST(NULLIF(wdsp, '999.9') AS FLOAT64) * 0.514444, 2) AS viento_ms
FROM `bigquery-public-data.noaa_gsod.gsod*`
WHERE _TABLE_SUFFIX BETWEEN '2011' AND '2014' AND stn = '037720';

-- 1 fila por medidor-día: STRUCT con el hogar y ARRAY<STRUCT> con sus 48 lecturas.
CREATE OR REPLACE TABLE `${P}.${DS}.consumo_dia_anidado`
PARTITION BY DATE_TRUNC(fecha, MONTH)
CLUSTER BY lclid AS
SELECT
  c.lclid,
  c.fecha,
  STRUCT(h.tarifa, h.acorn, h.acorn_grupo, h.zona, h.transformador) AS hogar,
  ROUND(SUM(c.kwh), 4) AS kwh_total,
  ROUND(SUM(IF(c.es_punta, c.kwh, 0)), 4) AS kwh_punta,
  COUNT(c.kwh) AS n_lecturas,
  ARRAY_AGG(STRUCT(c.periodo_local, c.ts_utc, c.kwh, c.es_nulo, c.es_punta) ORDER BY c.ts_utc) AS lecturas
FROM `${P}.${DS}.fact_consumo_30min` c
JOIN `${P}.${DS}.dim_hogar` h USING (lclid)
WHERE c.fecha IS NOT NULL
GROUP BY c.lclid, c.fecha, h.tarifa, h.acorn, h.acorn_grupo, h.zona, h.transformador;

-- Umbral de pico por zona, estación, tipo de día y media hora local, normalizado por medidor
-- (la cantidad de medidores activos cambia en el tiempo). Por estación: un p95 de todo el año
-- marcaba el 63 % de las ventanas de una semana fría; el de invierno, en hora punta, el 7 %.
-- Lo usa el streaming (streaming_job.py, consulta carga_zona).
CREATE OR REPLACE TABLE `${P}.${DS}.umbral_zona` AS
WITH carga AS (
  SELECT
    z.zona,
    f.tipo_dia,
    f.estacion,
    EXTRACT(HOUR FROM DATETIME(z.ts_utc, 'Europe/London')) * 2
      + DIV(EXTRACT(MINUTE FROM DATETIME(z.ts_utc, 'Europe/London')), 30) + 1 AS periodo_local,
    z.kw_medio / z.n_medidores AS kw_por_medidor
  FROM `${P}.${DS}.fact_carga_zona_30min` z
  JOIN `${P}.${DS}.dim_fecha` f USING (fecha)
  WHERE z.n_medidores >= 100
)
SELECT
  zona, tipo_dia, estacion, periodo_local,
  ROUND(APPROX_QUANTILES(kw_por_medidor, 100)[OFFSET(50)], 4) AS kw_p50,
  ROUND(APPROX_QUANTILES(kw_por_medidor, 100)[OFFSET(95)], 4) AS kw_p95,
  COUNT(*) AS n_observaciones
FROM carga
GROUP BY zona, tipo_dia, estacion, periodo_local;
