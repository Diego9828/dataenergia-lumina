#!/usr/bin/env bash
# Envía un job PySpark de src/batch/ a Dataproc Serverless (sin clúster encendido).
# Uso:
#   bash infra/05_spark_batch.sh silver [--muestra]   # bronze -> silver
#   bash infra/05_spark_batch.sh gold   [--muestra]   # silver -> gold
# --muestra procesa 2 bloques de medidores y escribe en gs://<curated>/muestra/ (centavos).
#
# Costo: US$0.06 por DCU-hora. Nodo de 4 vCPU/16 GiB = 4 DCU. Con máx. 4 ejecutores
# (driver + 4 = 20 DCU) cuesta ~US$1.20 por hora de ejecución, facturado por segundo.
# Al terminar se imprime el costo real medido por Dataproc.
source "$(dirname "$0")/_common.sh"
require_config

JOB="${1:?Uso: 05_spark_batch.sh silver|gold [--muestra]}"
MUESTRA="${2:-}"
[[ "$JOB" == silver || "$JOB" == gold ]] || die "Job desconocido: $JOB"

SA_NAME="lumina-spark"
SA_EMAIL="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
RUNTIME="2.2"   # Spark 3.5
MAX_EXECUTORS="${MAX_EXECUTORS:-4}"

PREFIJO="gs://$BUCKET_CURATED"
EXTRA=()
if [[ "$MUESTRA" == "--muestra" ]]; then
  PREFIJO="gs://$BUCKET_CURATED/muestra"
  EXTRA=(--muestra-bloques 2)
fi
case "$JOB" in
  silver) ENTRADA="gs://$BUCKET_RAW/kaggle"; SALIDA="$PREFIJO/silver"
          # Sincronización 1: en la corrida completa se consolida lo que el streaming guardó en bronze
          [[ "$MUESTRA" == "--muestra" ]] || EXTRA=(--entrada-streaming "gs://$BUCKET_RAW/streaming/lecturas") ;;
  gold)   ENTRADA="$PREFIJO/silver";          SALIDA="$PREFIJO/gold" ;;
esac

log "Cuenta de servicio de mínimo privilegio: $SA_EMAIL"
if ! gcloud iam service-accounts describe "$SA_EMAIL" --project "$PROJECT_ID" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_NAME" --project "$PROJECT_ID" \
    --display-name "Lumina · jobs Spark (Dataproc Serverless)"
  sleep 10
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --condition=None \
    --member "serviceAccount:$SA_EMAIL" --role roles/dataproc.worker >/dev/null
  gcloud storage buckets add-iam-policy-binding "gs://$BUCKET_RAW" \
    --member "serviceAccount:$SA_EMAIL" --role roles/storage.objectViewer >/dev/null
  gcloud storage buckets add-iam-policy-binding "gs://$BUCKET_CURATED" \
    --member "serviceAccount:$SA_EMAIL" --role roles/storage.objectAdmin >/dev/null
fi

BATCH_ID="lumina-$JOB-$(date -u +%Y%m%d-%H%M%S)"
# El código se sube a GCS antes de enviar: gcloud en Windows arma las rutas de
# --deps-bucket con "\" y Dataproc las rechaza.
CODIGO="gs://$BUCKET_CURATED/_codigo/$BATCH_ID"
log "Subiendo código a $CODIGO"
gcloud storage cp "$REPO_DIR/src/batch/$JOB.py" "$REPO_DIR/src/batch/lumina_common.py" "$CODIGO/" --quiet

log "Enviando $BATCH_ID: $ENTRADA -> $SALIDA (máx. $MAX_EXECUTORS ejecutores)"
gcloud dataproc batches submit pyspark "$CODIGO/$JOB.py" \
  --project "$PROJECT_ID" --region "$REGION" \
  --batch "$BATCH_ID" \
  --version "$RUNTIME" \
  --service-account "$SA_EMAIL" \
  --subnet default \
  --py-files "$CODIGO/lumina_common.py" \
  --ttl 2h \
  --labels "$LABELS,job=$JOB" \
  --properties "spark.executor.instances=2,spark.dynamicAllocation.maxExecutors=$MAX_EXECUTORS,spark.sql.session.timeZone=UTC,spark.sql.shuffle.partitions=128" \
  -- --entrada "$ENTRADA" --salida "$SALIDA" "${EXTRA[@]}" || estado=$?

gcloud dataproc batches describe "$BATCH_ID" --project "$PROJECT_ID" --region "$REGION" >/dev/null 2>&1 \
  || die "El batch $BATCH_ID no llegó a crearse (sin costo)."
log "Consumo de $BATCH_ID"
# Dataproc no siempre publica approximateUsage; en ese caso se usa una cota superior:
# duración × DCU máximos configurados (driver + MAX_EXECUTORS, 4 DCU por nodo de 4 vCPU/16 GiB).
read -r st creado fin mdcu < <(gcloud dataproc batches describe "$BATCH_ID" \
  --project "$PROJECT_ID" --region "$REGION" \
  --format "value(state,createTime,stateTime,runtimeInfo.approximateUsage.milliDcuSeconds)")
segundos=$(( $(date -d "$fin" +%s) - $(date -d "$creado" +%s) ))
awk -v id="$BATCH_ID" -v st="$st" -v s="$segundos" -v m="${mdcu:-}" -v dcu_max="$(( (MAX_EXECUTORS + 1) * 4 ))" 'BEGIN {
  if (m != "" && m + 0 > 0) { dcu_h = m / 1000 / 3600; tipo = "medido" } else { dcu_h = s / 3600 * dcu_max; tipo = "cota sup." }
  printf "%s  estado=%s  duracion=%.1f min  DCU-h=%.3f (%s)  costo<=US$%.3f\n", id, st, s / 60, dcu_h, tipo, dcu_h * 0.06
}' | tee -a "$REPO_DIR/docs/bitacora_costos.log"
exit "${estado:-0}"
