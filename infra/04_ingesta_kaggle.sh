#!/usr/bin/env bash
# Fase 1 · Paso 4: descarga Kaggle -> gs://<proyecto>-lumina-raw/kaggle/ usando una VM efímera.
# La descarga ocurre dentro de GCP (ingress gratis, sin usar el ancho de banda de tu laptop).
# Costo: e2-standard-2 (US$0.067/h) + 100 GB pd-standard durante ~1 h  ≈ US$0.10.
# Red de seguridad: la VM se BORRA sola a las 3 h (--max-run-duration) aunque algo falle.
#
# Prerrequisito: el secreto con el token de Kaggle (bash infra/03_secreto_kaggle.sh).
source "$(dirname "$0")/_common.sh"
require_config

SA_NAME="lumina-ingest"
SA_EMAIL="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"

log "Verificando el secreto $KAGGLE_SECRET"
gcloud secrets describe "$KAGGLE_SECRET" --project "$PROJECT_ID" >/dev/null 2>&1 || die \
  "No existe el secreto '$KAGGLE_SECRET'. Créalo con: bash infra/03_secreto_kaggle.sh"

log "Cuenta de servicio de mínimo privilegio: $SA_EMAIL"
if ! gcloud iam service-accounts describe "$SA_EMAIL" --project "$PROJECT_ID" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_NAME" --project "$PROJECT_ID" \
    --display-name "Lumina · ingesta Kaggle (VM efímera)"
  sleep 10  # propagación de IAM
fi
gcloud secrets add-iam-policy-binding "$KAGGLE_SECRET" --project "$PROJECT_ID" \
  --member "serviceAccount:$SA_EMAIL" --role roles/secretmanager.secretAccessor --condition=None >/dev/null
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET_RAW" \
  --member "serviceAccount:$SA_EMAIL" --role roles/storage.objectAdmin >/dev/null

if gcloud compute instances describe "$INGEST_VM" --zone "$ZONE" --project "$PROJECT_ID" >/dev/null 2>&1; then
  echo "La VM $INGEST_VM ya existe; solo se espera a que termine."
else
  log "Creando VM efímera $INGEST_VM"
  gcloud compute instances create "$INGEST_VM" \
    --project "$PROJECT_ID" --zone "$ZONE" \
    --machine-type e2-standard-2 \
    --image-family debian-12 --image-project debian-cloud \
    --boot-disk-size 100GB --boot-disk-type pd-standard \
    --service-account "$SA_EMAIL" --scopes cloud-platform \
    --max-run-duration 3h --instance-termination-action DELETE \
    --labels "$LABELS" \
    --metadata "project-id=$PROJECT_ID,bucket-raw=$BUCKET_RAW,kaggle-secret=$KAGGLE_SECRET" \
    --metadata-from-file "startup-script=$REPO_DIR/src/ingestion/kaggle_to_gcs.sh"
fi

log "Esperando a que la VM reporte gs://$BUCKET_RAW/_logs/ingest_status.txt (revisión cada 60 s)"
status=""
for _ in $(seq 1 180); do
  status="$(gcloud storage cat "gs://$BUCKET_RAW/_logs/ingest_status.txt" 2>/dev/null || true)"
  [[ -n "$status" ]] && break
  printf '.'
  sleep 60
done
echo
echo "Estado: ${status:-SIN RESPUESTA}"

log "Borrando la VM $INGEST_VM (los datos ya están en GCS)"
gcloud compute instances delete "$INGEST_VM" --zone "$ZONE" --project "$PROJECT_ID" --quiet || true

[[ "$status" == OK* ]] || die "La ingesta no terminó bien. Revisa gs://$BUCKET_RAW/_logs/lumina-ingest.log"

log "Contenido de la capa raw"
gcloud storage du -s -r "gs://$BUCKET_RAW/kaggle/"
gcloud storage ls "gs://$BUCKET_RAW/kaggle/*/"
