# Fase 2 – Modelo de datos y reglas de limpieza

> Generado con asistencia de IA (Claude). Validar supuestos con el docente.

## Capas

| Capa | Ubicación | Formato | Contenido |
|---|---|---|---|
| bronze | `gs://<proyecto>-lumina-raw/kaggle/` | CSV original | Copia fiel de Kaggle (Fase 1) |
| silver | `gs://<proyecto>-lumina-curated/silver/` | Parquet | Tipado, nombres en español, UTC, banderas de calidad; sin agregar |
| gold | `gs://<proyecto>-lumina-curated/gold/` | Parquet | Modelo estrella, agregados, perfiles y calidad por medidor |
| warehouse | BigQuery `lumina_curated` | tablas nativas | Carga de gold (gratis) + clima NOAA GSOD |

Jobs: [src/batch/silver.py](../src/batch/silver.py) (bronze → silver) y [src/batch/gold.py](../src/batch/gold.py) (silver → gold), en Dataproc Serverless.

## Modelo estrella (gold)

```mermaid
erDiagram
  dim_hogar ||--o{ fact_consumo_30min : lclid
  dim_hogar ||--o{ fact_consumo_diario : lclid
  dim_hogar ||--o{ perfil_hogar : lclid
  dim_hogar ||--|| dq_medidor : lclid
  dim_acorn ||--o{ dim_hogar : acorn
  dim_acorn ||--o{ acorn_atributos : acorn
  dim_fecha ||--o{ fact_consumo_30min : fecha
  dim_fecha ||--o{ fact_consumo_diario : fecha
  dim_fecha ||--o{ fact_clima_dia : fecha
  dim_fecha ||--o{ fact_demanda_nacional_30min : fecha
  dim_fecha ||--o{ fact_carga_transformador_30min : fecha

  dim_hogar {
    string lclid PK
    string tarifa "Std | ToU"
    string acorn FK
    string acorn_grupo
    int zona "1-8 (sintetica)"
    string transformador "Zx-Tyy (sintetico)"
  }
  dim_acorn {
    string acorn PK
    string acorn_grupo
  }
  acorn_atributos {
    string acorn FK
    string categoria_principal
    string categoria
    string referencia
    double indice "100 = media UK"
  }
  dim_fecha {
    date fecha PK
    int dia_semana
    bool es_fin_de_semana
    bool es_feriado
    string tipo_dia "laborable | no_laborable"
    string estacion
  }
  fact_consumo_30min {
    string lclid FK
    timestamp ts_utc
    date fecha FK
    int periodo_local "1-48, hora de Londres"
    double kwh
    bool es_nulo
    bool es_punta "17:00-20:59 local"
  }
  fact_consumo_diario {
    string lclid FK
    date fecha FK
    double kwh_total
    double kwh_punta
    double kwh_max
    int n_lecturas
    double cobertura
  }
  fact_carga_transformador_30min {
    int zona
    string transformador
    timestamp ts_utc
    date fecha FK
    double kwh_total
    int n_medidores
  }
  perfil_hogar {
    string lclid FK
    string tipo_dia
    int periodo_local
    double kwh_media
    double kwh_std
    double kwh_p95
  }
  dq_medidor {
    string lclid PK
    double cobertura_pct
    int n_huecos
    int max_racha_ceros
    string diagnostico
  }
  fact_demanda_nacional_30min {
    timestamp ts_utc
    date fecha FK
    int periodo_liquidacion
    double nd_mw
    double eolica_embebida_mw
    double solar_embebida_mw
  }
  fact_clima_dia {
    date fecha PK
    double temp_max
    double temp_min
  }
```

También: `fact_clima_hora` (DarkSky horario), `fact_carga_zona_30min` (agregado por zona) y, en BigQuery, `clima_noaa_heathrow` (GSOD, estación 037720).

## Reglas de limpieza (de perfilar la muestra)

| Hallazgo | Regla |
|---|---|
| `energy(kWh/hh)` llega como texto con espacios (`" 0 "`) y `Null` | `trim` + cast a double; `Null` → `kwh = NULL`, `es_nulo = true` |
| `tstp` con 7 decimales | se parsean los primeros 19 caracteres |
| Lecturas fuera de la grilla de 30 min (ej. `15:13:41`) | `fuera_de_grilla = true` en silver; se excluyen de gold y se cuentan en `dq_medidor` |
| Duplicados `(lclid, ts)` | se conserva uno; el conteo queda en `dq_medidor` |
| Lecturas de medidores en UTC (48 por día incluso en cambio de hora) | `ts_utc = tstp`; hora local de Londres solo para `periodo_local`, `es_punta` y perfiles |
| Demanda nacional en hora local (46/50 periodos en cambio de hora) | `ts_utc = to_utc(fecha 00:00 Europe/London) + (periodo − 1) × 30 min` |
| `Acorn` = `ACORN-` o `ACORN-U` (51 hogares) | `acorn = 'DESCONOCIDO'`, `acorn_grupo = 'Desconocido'` |
| `acorn_details.csv` en ISO-8859-1 (símbolo £) | se lee con `encoding = ISO-8859-1` y se pasa a formato largo |
| Clima DarkSky horario con 13 `pressure` nulos | se conservan nulos |

## Supuestos declarados

- `tstp` marca el **inicio** del intervalo de 30 min.
- La hora de DarkSky (`time`) se asume en UTC.
- **Zonas y transformadores sintéticos**: el dataset no trae geografía. `zona = pmod(xxhash64(lclid), 8) + 1` y 14 transformadores por zona (~50 medidores c/u), con hash determinístico (reproducible).
- **Hora punta**: 17:00–20:59 hora de Londres (equivale a la franja 18–21 h del caso).
- **Tipo de día**: no laborable = sábado, domingo o feriado bancario UK.

## Diagnóstico de medidores (`dq_medidor`)

| Diagnóstico | Condición (en orden de prioridad) |
|---|---|
| `sin_datos` | 0 lecturas válidas |
| `ceros_prolongados` | racha de ceros ≥ 48 intervalos (24 h) |
| `cobertura_baja` | cobertura < 90 % del periodo activo |
| `huecos_frecuentes` | > 10 huecos de telemetría |
| `ok` | resto |

## Resultados de la corrida completa (2026-10-20)

Motor: **Managed Service for Apache Spark** (ex Dataproc Serverless), runtime 2.2 / Spark 3.5, máx. 4 ejecutores.

| Job | Duración | Costo (cota superior) |
|---|---|---|
| silver (112 bloques) | 10.6 min | US$0.21 |
| gold | 21.8 min | US$0.44 |
| Muestras de prueba (2 bloques) | 8.1 min | US$0.16 |

Detalle en [bitacora_costos.log](bitacora_costos.log).

### Volúmenes en BigQuery (`lumina_curated`)

| Tabla | Filas | GiB |
|---|---:|---:|
| fact_consumo_30min | 167,811,461 | 8.28 |
| consumo_dia_anidado (STRUCT/ARRAY) | 3,510,403 | 4.34 |
| fact_consumo_diario | 3,510,403 | 0.25 |
| fact_carga_transformador_30min | 4,394,004 | 0.23 |
| perfil_hogar | 533,804 | 0.04 |
| fact_carga_zona_30min | 317,746 | 0.01 |
| fact_demanda_nacional_30min | 279,264 | 0.02 |
| dims, clima DarkSky/NOAA, dq_medidor | < 25 k c/u | < 0.01 |

### Validación contra Kaggle `daily_dataset`

- 3,510,403 medidor-día coinciden **exactamente** en kWh y número de lecturas; total 35.54 GWh en ambos lados.
- 30 días existen solo en Kaggle con `energy_count = 0`: su única lectura era el `Null` fuera de grilla que excluimos a propósito.
- 5,566 hogares en el maestro, 5,561 con lecturas (5 sin datos).

### Calidad de medidores (`dq_medidor`)

| Diagnóstico | Medidores | Cobertura media |
|---|---:|---:|
| ok | 5,068 | 99.8 % |
| huecos_frecuentes | 240 | 98.8 % |
| ceros_prolongados (≥ 24 h en cero) | 232 | 99.6 % |
| cobertura_baja (< 90 %) | 21 | 82.8 % |

### Plano vs. anidado (bytes escaneados, dry run)

| Consulta | Plano | STRUCT/ARRAY | Ahorro |
|---|---:|---:|---:|
| Curva de 1 medidor, enero 2013 | 273 MB | 128 MB | 2.1× |
| kWh/día por grupo Acorn, todo el histórico | 4,321 MB (con JOIN) | 91 MB | 48× |

El dry run no descuenta la poda por clustering, así que el ahorro real en la consulta de un medidor es menor. La tabla plana se mantiene para Looker Studio y para las inserciones de streaming; la anidada sirve para análisis por medidor-día.
