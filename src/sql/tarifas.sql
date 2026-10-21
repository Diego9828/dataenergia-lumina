-- Fase 4 · Tarifa dinámica (dToU) vs. tarifa plana, 2013 (vertical III · informes).
-- Usa el calendario oficial (lumina_curated.dim_tarifa_dtou) y el consumo real cada 30 min.
-- Uso: bash infra/11_tarifas.sh      ${P} = proyecto

-- 1 · Factura anual 2013 de cada hogar con ambas tarifas (mismo consumo real).
--     Solo hogares con al menos 90 % de las 17,520 medias horas del año.
CREATE OR REPLACE TABLE `${P}.lumina_ml.factura_tarifas_2013` AS
SELECT
  c.lclid, h.tarifa, h.acorn_grupo,
  COUNT(*) AS lecturas,
  ROUND(SUM(c.kwh), 2) AS kwh_2013,
  ROUND(SUM(c.kwh * t.precio_p_kwh) / 100, 2) AS costo_dtou_gbp,
  ROUND(SUM(c.kwh * t.precio_plano_p_kwh) / 100, 2) AS costo_plano_gbp,
  ROUND(SAFE_DIVIDE(SUM(IF(t.nivel = 'High', c.kwh, 0)), SUM(c.kwh)) * 100, 2) AS pct_kwh_en_high,
  ROUND(SAFE_DIVIDE(SUM(IF(t.nivel = 'Low', c.kwh, 0)), SUM(c.kwh)) * 100, 2) AS pct_kwh_en_low
FROM `${P}.lumina_curated.fact_consumo_30min` c
JOIN `${P}.lumina_curated.dim_tarifa_dtou` t USING (ts_utc)
JOIN `${P}.lumina_curated.dim_hogar` h USING (lclid)
WHERE c.fecha BETWEEN '2013-01-01' AND '2013-12-31' AND c.kwh IS NOT NULL
GROUP BY c.lclid, h.tarifa, h.acorn_grupo
HAVING COUNT(*) >= 17520 * 0.9 AND SUM(c.kwh) > 0;   -- excluye medidores con todo el año en cero

-- 2 · Respuesta a la demanda: consumo durante eventos High/Low relativo al consumo normal del mismo
--     hogar, a la misma media hora local y en el mismo mes (días con tarifa Normal).
--     Los hogares Std no recibieron señales: son el grupo de control.
CREATE OR REPLACE TABLE `${P}.lumina_ml.respuesta_eventos_2013` AS
WITH lecturas AS (
  SELECT c.lclid, h.tarifa, c.kwh, c.periodo_local, EXTRACT(MONTH FROM c.fecha) AS mes, t.nivel
  FROM `${P}.lumina_curated.fact_consumo_30min` c
  JOIN `${P}.lumina_curated.dim_tarifa_dtou` t USING (ts_utc)
  JOIN `${P}.lumina_curated.dim_hogar` h USING (lclid)
  WHERE c.fecha BETWEEN '2013-01-01' AND '2013-12-31' AND c.kwh IS NOT NULL
    AND c.lclid IN (SELECT lclid FROM `${P}.lumina_ml.factura_tarifas_2013`)
),
normal AS (
  SELECT lclid, mes, periodo_local, AVG(kwh) AS kwh_normal
  FROM lecturas WHERE nivel = 'Normal'
  GROUP BY lclid, mes, periodo_local
)
SELECT l.tarifa, l.nivel, COUNT(DISTINCT l.lclid) AS hogares, COUNT(*) AS medias_horas,
       ROUND(SAFE_DIVIDE(SUM(l.kwh), SUM(n.kwh_normal)), 4) AS consumo_relativo
FROM lecturas l
JOIN normal n USING (lclid, mes, periodo_local)
WHERE l.nivel IN ('High', 'Low')
GROUP BY l.tarifa, l.nivel;
