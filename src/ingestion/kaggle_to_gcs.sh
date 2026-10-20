#!/usr/bin/env bash
# Startup script de la VM efímera lumina-ingest (corre como root al arrancar).
# Descarga los datasets de Kaggle y los copia SIN transformar a la capa raw (bronze).
# Lee el token de Kaggle desde Secret Manager; nunca queda en el repositorio ni en GCS.
set -euo pipefail
exec > >(tee -a /var/log/lumina-ingest.log) 2>&1

md() { curl -sf -H 'Metadata-Flavor: Google' \
  "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"; }

PROJECT_ID="$(md project-id)"
BUCKET_RAW="$(md bucket-raw)"
KAGGLE_SECRET="$(md kaggle-secret)"
STATUS_URI="gs://$BUCKET_RAW/_logs/ingest_status.txt"

report() {
  echo "$1 $(date -u +%FT%TZ)" > /tmp/status.txt
  gcloud storage cp /tmp/status.txt "$STATUS_URI" --quiet
  gcloud storage cp /var/log/lumina-ingest.log "gs://$BUCKET_RAW/_logs/lumina-ingest.log" --quiet || true
}
trap 'report ERROR' ERR

# Evita re-ejecutar la descarga si la VM se reinicia.
if gcloud storage ls "$STATUS_URI" >/dev/null 2>&1 && gcloud storage cat "$STATUS_URI" | grep -q '^OK'; then
  echo "Ingesta ya completada; nada que hacer."
  exit 0
fi

export HOME=/root
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq python3-venv unzip
python3 -m venv /opt/kaggle
/opt/kaggle/bin/pip install -q kaggle

# El secreto puede ser un API token nuevo (KGAT_...) o un kaggle.json clásico.
mkdir -p /root/.kaggle
export KAGGLE_CONFIG_DIR=/root/.kaggle
kaggle_cred="$(gcloud secrets versions access latest --secret "$KAGGLE_SECRET" --project "$PROJECT_ID")"
if [[ "$kaggle_cred" == \{* ]]; then
  printf '%s' "$kaggle_cred" > /root/.kaggle/kaggle.json
  chmod 600 /root/.kaggle/kaggle.json
else
  export KAGGLE_API_TOKEN="$kaggle_cred"
fi
unset kaggle_cred

download() {  # $1 = dataset de Kaggle, $2 = carpeta destino en raw
  local dir="/data/$2"
  mkdir -p "$dir"
  /opt/kaggle/bin/kaggle datasets download -d "$1" -p "$dir" --unzip
  # Algunos datasets traen zips anidados (p. ej. halfhourly_dataset.zip)
  find "$dir" -name '*.zip' -print0 | while IFS= read -r -d '' z; do
    unzip -q -o "$z" -d "$(dirname "$z")" && rm -f "$z"
  done
  du -sh "$dir"
  gcloud storage cp -r "$dir"/* "gs://$BUCKET_RAW/kaggle/$2/" --quiet
}

download jeanmidev/smart-meters-in-london            smart_meters_london
download albertovidalrod/electricity-consumption-uk-20092022 uk_national_demand

rm -f /root/.kaggle/kaggle.json; unset KAGGLE_API_TOKEN
gcloud storage du -s -r "gs://$BUCKET_RAW/kaggle/" || true
report OK
echo "Ingesta terminada. La VM la borra infra/04_ingesta_kaggle.sh."
