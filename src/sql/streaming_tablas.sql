-- Fase 3 · Tablas de salida del streaming (dataset lumina_rt). Uso: infra/07_vm_ops.sh tablas
-- ${P} se reemplaza por el ID del proyecto.

-- El umbral de picos por zona (umbral_zona) está en gold_derivadas.sql.

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
