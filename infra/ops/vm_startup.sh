#!/usr/bin/env bash
# Startup script de la VM lumina-ops (corre como root en CADA arranque).
# 1) Instala Docker la primera vez; 2) baja el bundle de código desde GCS;
# 3) levanta Kafka + Spark Structured Streaming con Docker Compose.
set -euo pipefail
exec > >(tee -a /var/log/lumina-ops.log) 2>&1
echo "== arranque $(date -u +%FT%TZ)"

md() { curl -sf -H 'Metadata-Flavor: Google' \
  "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"; }
PROYECTO="$(md project-id)"
BUCKET_CURATED="$(md bucket-curated)"

if ! command -v docker >/dev/null; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq ca-certificates curl
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -qq
  apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

mkdir -p /opt/lumina
gcloud storage cp "gs://$BUCKET_CURATED/_ops/bundle.tgz" /tmp/bundle.tgz --quiet
tar -xzf /tmp/bundle.tgz -C /opt/lumina
printf 'PROYECTO=%s\nBUCKET_CURATED=%s\n' "$PROYECTO" "$BUCKET_CURATED" > /opt/lumina/infra/ops/.env

cd /opt/lumina/infra/ops
docker compose up -d --build --remove-orphans
docker compose build replay
echo "== stack arriba $(date -u +%FT%TZ)"
docker compose ps
