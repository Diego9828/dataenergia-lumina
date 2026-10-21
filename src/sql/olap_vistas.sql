-- Fase 4 · Capa de servicio: vistas lumina_olap (sincronización 3 y 4 de la arquitectura Lambda).
-- Las vistas no guardan datos: leen lumina_curated (batch, definitivo) y lumina_rt (streaming, provisional).
-- Uso: bash infra/09_vistas_olap.sh      ${P} = proyecto

-- Carga por zona cada 30 min. Punto 3: une batch y streaming. Punto 4: del streaming solo se toma
-- lo posterior a lo último que procesó el batch; si ambos tienen la misma ventana, manda el batch.
-- La regla de pico es la misma del streaming: kW por medidor > p95 de la estación, solo 17-21 h.
CREATE OR REPLACE VIEW `${P}.lumina_olap.v_carga_zona_30min` AS
WITH batch AS (
  SELECT ts_utc, fecha, zona, kwh_total, n_medidores, kw_medio / n_medidores AS kw_por_medidor,
         'batch' AS fuente
  FROM `${P}.lumina_curated.fact_carga_zona_30min`
),
corte AS (SELECT MAX(ts_utc) AS ultimo_batch FROM batch),
rt AS (
  SELECT ventana_inicio AS ts_utc, fecha, zona, kwh_total, n_lecturas AS n_medidores, kw_por_medidor,
         'streaming' AS fuente
  FROM `${P}.lumina_rt.carga_zona_30min`
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY zona, ventana_inicio ORDER BY ts_procesado DESC) = 1
),
unida AS (
  SELECT * FROM batch
  UNION ALL
  SELECT rt.* FROM rt CROSS JOIN corte WHERE rt.ts_utc > corte.ultimo_batch
)
SELECT
  u.ts_utc, u.fecha, u.zona, u.kwh_total, u.n_medidores, ROUND(u.kw_por_medidor, 4) AS kw_por_medidor,
  u.fuente,
  EXTRACT(HOUR FROM DATETIME(u.ts_utc, 'Europe/London')) AS hora_local,
  f.tipo_dia, f.estacion, t.kw_p95,
  COALESCE(u.kw_por_medidor > t.kw_p95
           AND EXTRACT(HOUR FROM DATETIME(u.ts_utc, 'Europe/London')) BETWEEN 17 AND 20, FALSE) AS es_pico
FROM unida u
LEFT JOIN `${P}.lumina_curated.dim_fecha` f USING (fecha)
LEFT JOIN `${P}.lumina_curated.umbral_zona` t
  ON t.zona = u.zona AND t.tipo_dia = f.tipo_dia AND t.estacion = f.estacion
 AND t.periodo_local = EXTRACT(HOUR FROM DATETIME(u.ts_utc, 'Europe/London')) * 2
                       + DIV(EXTRACT(MINUTE FROM DATETIME(u.ts_utc, 'Europe/London')), 30) + 1;

-- Consumo diario por segmento Acorn y tarifa (vertical III). Privacidad: solo grupos de 10 hogares o más.
CREATE OR REPLACE VIEW `${P}.lumina_olap.v_consumo_segmento_diario` AS
SELECT
  d.fecha, f.tipo_dia, f.estacion, h.acorn_grupo, h.tarifa,
  COUNT(DISTINCT d.lclid) AS hogares,
  ROUND(AVG(d.kwh_total), 3) AS kwh_medio_hogar,
  ROUND(SAFE_DIVIDE(SUM(d.kwh_punta), SUM(d.kwh_total)) * 100, 1) AS pct_en_punta
FROM `${P}.lumina_curated.fact_consumo_diario` d
JOIN `${P}.lumina_curated.dim_hogar` h USING (lclid)
JOIN `${P}.lumina_curated.dim_fecha` f USING (fecha)
WHERE d.cobertura >= 0.9
GROUP BY d.fecha, f.tipo_dia, f.estacion, h.acorn_grupo, h.tarifa
HAVING COUNT(DISTINCT d.lclid) >= 10;

-- Alertas del streaming sin duplicados (reinicios con checkpoint pueden reescribir un lote), con latencia.
CREATE OR REPLACE VIEW `${P}.lumina_olap.v_alertas` AS
SELECT
  a.ts_evento, a.fecha, a.tipo, a.lclid, a.zona, a.transformador, a.valor, a.umbral,
  h.tarifa, h.acorn_grupo,
  ROUND(TIMESTAMP_DIFF(a.ts_procesado, a.ts_kafka, MILLISECOND) / 1000, 1) AS latencia_s,
  a.ts_procesado
FROM `${P}.lumina_rt.alertas` a
LEFT JOIN `${P}.lumina_curated.dim_hogar` h USING (lclid)
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY a.tipo, COALESCE(a.lclid, CAST(a.zona AS STRING)), a.ts_evento ORDER BY a.ts_procesado DESC) = 1;

-- Calidad por medidor (base del mantenimiento predictivo, vertical IV).
CREATE OR REPLACE VIEW `${P}.lumina_olap.v_calidad_medidores` AS
SELECT
  q.lclid, h.zona, h.transformador, h.tarifa, h.acorn_grupo,
  q.diagnostico, q.cobertura_pct, q.n_huecos, q.max_hueco_intervalos, q.max_racha_ceros,
  q.n_nulos, q.n_fuera_de_grilla, q.primera_lectura, q.ultima_lectura
FROM `${P}.lumina_curated.dq_medidor` q
LEFT JOIN `${P}.lumina_curated.dim_hogar` h USING (lclid);

-- Demanda nacional frente a la carga de la muestra, cada 30 min (contexto de red).
CREATE OR REPLACE VIEW `${P}.lumina_olap.v_red_vs_muestra` AS
WITH muestra AS (
  SELECT ts_utc, SUM(kwh_total) AS kwh_muestra, SUM(n_medidores) AS medidores
  FROM `${P}.lumina_curated.fact_carga_zona_30min`
  GROUP BY ts_utc
)
SELECT
  n.ts_utc, n.fecha, n.nd_mw, n.eolica_embebida_mw, n.solar_embebida_mw, n.es_feriado,
  m.medidores, ROUND(m.kwh_muestra * 2 / m.medidores, 4) AS kw_por_medidor_muestra
FROM `${P}.lumina_curated.fact_demanda_nacional_30min` n
JOIN muestra m USING (ts_utc);
