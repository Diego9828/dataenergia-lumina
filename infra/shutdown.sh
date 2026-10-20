#!/usr/bin/env bash
# Apaga todo lo que cobra por hora. CONSERVA los datos (GCS, BigQuery, discos).
# Ejecutar al final de cada sesión de trabajo: bash infra/shutdown.sh
source "$(dirname "$0")/_common.sh"
require_config

log "VMs del proyecto con label proyecto=lumina"
gcloud compute instances list --project "$PROJECT_ID" --filter "labels.proyecto=lumina" \
  --format "table(name,zone.basename(),status)"

if gcloud compute instances describe "$INGEST_VM" --zone "$ZONE" --project "$PROJECT_ID" >/dev/null 2>&1; then
  log "Borrando VM efímera $INGEST_VM"
  gcloud compute instances delete "$INGEST_VM" --zone "$ZONE" --project "$PROJECT_ID" --quiet
fi

if gcloud compute instances describe "$OPS_VM" --zone "$ZONE" --project "$PROJECT_ID" >/dev/null 2>&1; then
  log "Deteniendo $OPS_VM (el disco se conserva, ~US\$0.10/día)"
  gcloud compute instances stop "$OPS_VM" --zone "$ZONE" --project "$PROJECT_ID" --quiet
fi

log "Cancelando lotes de Dataproc Serverless en ejecución"
for batch in $(gcloud dataproc batches list --project "$PROJECT_ID" --region "$REGION" \
    --filter "state=RUNNING OR state=PENDING" --format "value(name.basename())"); do
  gcloud dataproc batches cancel "$batch" --project "$PROJECT_ID" --region "$REGION" --quiet
done

log "Clústeres de Dataproc (no deberían existir; se usa Serverless)"
gcloud dataproc clusters list --project "$PROJECT_ID" --region "$REGION" --format "table(clusterName,status.state)"

log "Estado final"
gcloud compute instances list --project "$PROJECT_ID" --format "table(name,status)"
