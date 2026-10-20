#!/usr/bin/env bash
# Fase 3 · VM lumina-ops (Kafka + Spark Structured Streaming en Docker Compose).
#
#   bash infra/07_vm_ops.sh tablas      # umbrales + tablas lumina_rt en BigQuery (sin VM)
#   bash infra/07_vm_ops.sh crear       # cuenta de servicio, horario de apagado y VM (la ENCIENDE)
#   bash infra/07_vm_ops.sh desplegar   # sube el código a GCS y reinicia el stack en la VM
#   bash infra/07_vm_ops.sh iniciar     # enciende la VM (el stack arranca solo)
#   bash infra/07_vm_ops.sh replay [args del productor]   # p. ej. --dias 2 --anomalias 20
#   bash infra/07_vm_ops.sh logs [servicio]
#   bash infra/07_vm_ops.sh estado
#   bash infra/07_vm_ops.sh detener     # APAGA la VM (conserva el disco, ~US$3/mes)
#   bash infra/07_vm_ops.sh destruir    # BORRA la VM y su disco (costo 0 entre demos)
#
# Costo: e2-standard-4 US$0.134/h encendida; disco 30 GB pd-balanced ≈ US$3/mes aunque esté apagada.
# Red de seguridad: horario que apaga la VM todos los días a las 23:00 (hora de Lima).
source "$(dirname "$0")/_common.sh"
require_config

SA_NAME="lumina-ops"
SA_EMAIL="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
HORARIO="lumina-apagado-nocturno"
VM=(--project "$PROJECT_ID" --zone "$ZONE")

ssh_vm() { gcloud compute ssh "$OPS_VM" "${VM[@]}" --quiet --command "$1"; }

subir_bundle() {
  local tmp; tmp="$(mktemp -d)"
  tar -czf "$tmp/bundle.tgz" -C "$REPO_DIR" infra/ops src/streaming
  gcloud storage cp "$tmp/bundle.tgz" "gs://$BUCKET_CURATED/_ops/bundle.tgz" --quiet
  rm -rf "$tmp"
}

case "${1:-}" in
  tablas)
    sed "s/\${P}/$PROJECT_ID/g" "$REPO_DIR/src/sql/streaming_tablas.sql" |
      bq --project_id "$PROJECT_ID" --location "$BQ_LOCATION" query --nouse_legacy_sql
    ;;

  crear)
    log "Cuenta de servicio $SA_EMAIL"
    if ! gcloud iam service-accounts describe "$SA_EMAIL" --project "$PROJECT_ID" >/dev/null 2>&1; then
      gcloud iam service-accounts create "$SA_NAME" --project "$PROJECT_ID" \
        --display-name "Lumina · VM de streaming (Kafka + Spark)"
      sleep 10
    fi
    for rol in roles/bigquery.jobUser roles/bigquery.readSessionUser roles/bigquery.dataViewer \
               roles/logging.logWriter; do
      gcloud projects add-iam-policy-binding "$PROJECT_ID" --condition=None \
        --member "serviceAccount:$SA_EMAIL" --role "$rol" >/dev/null
    done
    # Escritura solo en lumina_rt: binding del dataset o, si bq no lo soporta, condición IAM.
    bq --project_id "$PROJECT_ID" add-iam-policy-binding \
      --member "serviceAccount:$SA_EMAIL" --role roles/bigquery.dataEditor "$PROJECT_ID:lumina_rt" >/dev/null 2>&1 \
    || gcloud projects add-iam-policy-binding "$PROJECT_ID" \
      --member "serviceAccount:$SA_EMAIL" --role roles/bigquery.dataEditor \
      --condition "title=solo_lumina_rt,expression=resource.name.startsWith(\"projects/$PROJECT_ID/datasets/lumina_rt\")" >/dev/null
    gcloud storage buckets add-iam-policy-binding "gs://$BUCKET_CURATED" \
      --member "serviceAccount:$SA_EMAIL" --role roles/storage.objectAdmin >/dev/null

    log "Horario $HORARIO: apaga la VM a las 23:00 (America/Lima)"
    if ! gcloud compute resource-policies describe "$HORARIO" --project "$PROJECT_ID" --region "$REGION" >/dev/null 2>&1; then
      gcloud compute resource-policies create instance-schedule "$HORARIO" \
        --project "$PROJECT_ID" --region "$REGION" \
        --vm-stop-schedule "0 23 * * *" --timezone "America/Lima"
      # El agente de servicio de Compute necesita permiso para detener la VM.
      numero="$(gcloud projects describe "$PROJECT_ID" --format 'value(projectNumber)')"
      gcloud projects add-iam-policy-binding "$PROJECT_ID" --condition=None \
        --member "serviceAccount:service-$numero@compute-system.iam.gserviceaccount.com" \
        --role roles/compute.instanceAdmin.v1 >/dev/null
    fi

    log "Subiendo código y creando $OPS_VM (queda ENCENDIDA)"
    subir_bundle
    gcloud compute instances create "$OPS_VM" "${VM[@]}" \
      --machine-type e2-standard-4 \
      --image-family debian-12 --image-project debian-cloud \
      --boot-disk-size 30GB --boot-disk-type pd-balanced \
      --service-account "$SA_EMAIL" --scopes cloud-platform \
      --resource-policies "$HORARIO" \
      --labels "$LABELS" \
      --metadata "project-id=$PROJECT_ID,bucket-curated=$BUCKET_CURATED" \
      --metadata-from-file "startup-script=$REPO_DIR/infra/ops/vm_startup.sh"
    echo "El primer arranque instala Docker y construye imágenes (~5-8 min): bash infra/07_vm_ops.sh logs"
    ;;

  desplegar)
    subir_bundle
    ssh_vm "sudo google_metadata_script_runner startup"
    ;;

  iniciar)
    gcloud compute instances start "$OPS_VM" "${VM[@]}"
    ;;

  replay)
    shift
    ssh_vm "cd /opt/lumina/infra/ops && sudo docker compose run --rm replay --proyecto $PROJECT_ID $*"
    ;;

  logs)
    ssh_vm "cd /opt/lumina/infra/ops && sudo docker compose logs --tail 80 ${2:-} 2>/dev/null || sudo tail -n 60 /var/log/lumina-ops.log"
    ;;

  estado)
    gcloud compute instances describe "$OPS_VM" "${VM[@]}" --format "value(name,status)"
    ssh_vm "cd /opt/lumina/infra/ops && sudo docker compose ps" || true
    ;;

  detener)
    gcloud compute instances stop "$OPS_VM" "${VM[@]}" --quiet
    ;;

  destruir)
    # Borra VM y disco: costo 0 entre demos. Todo el estado durable está en GCS/BigQuery
    # y "crear" reconstruye el stack (~8 min). Conserva la cuenta de servicio y el horario.
    gcloud compute instances delete "$OPS_VM" "${VM[@]}" --quiet
    ;;

  *)
    sed -n '2,16p' "$0"; exit 1 ;;
esac
