#!/usr/bin/env bash
# Funciones compartidas por los scripts de infra/. Uso: source "$(dirname "$0")/_common.sh"
set -euo pipefail

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$INFRA_DIR/.." && pwd)"
# config.env tiene los IDs reales y NO se versiona; se crea copiando config.example.env.
[[ -f "$INFRA_DIR/config.env" ]] || {
  echo "Falta infra/config.env. Créalo con: cp infra/config.example.env infra/config.env" >&2
  exit 1
}
# shellcheck source=config.example.env
source "$INFRA_DIR/config.env"
# Cuota de las APIs contra el proyecto propio, no contra el cliente compartido de gcloud
# (evita RESOURCE_EXHAUSTED en cloudresourcemanager cuando ese cliente está saturado).
[[ "$PROJECT_ID" == CAMBIAR* ]] || export CLOUDSDK_BILLING_QUOTA_PROJECT="${CLOUDSDK_BILLING_QUOTA_PROJECT:-$PROJECT_ID}"

# Git Bash en Windows: evita que gcloud tome el alias "python3" de Microsoft Store.
if [[ -z "${CLOUDSDK_PYTHON:-}" && "$(uname -s)" == MINGW* ]]; then
  _sdk_python="$(dirname "$(dirname "$(command -v gcloud)")")/platform/bundledpython/python.exe"
  [[ -x "$_sdk_python" ]] && export CLOUDSDK_PYTHON="$_sdk_python"
fi

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
die() { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

require_config() {
  [[ "$PROJECT_ID" != CAMBIAR* ]] || die "Define PROJECT_ID en infra/config.env o como variable de entorno."
  gcloud projects describe "$PROJECT_ID" --format='value(projectId)' >/dev/null \
    || die "No tengo acceso al proyecto $PROJECT_ID con la cuenta activa de gcloud."
}

require_billing() {
  [[ "$BILLING_ACCOUNT" != CAMBIAR* ]] || die "Define BILLING_ACCOUNT en infra/config.env o como variable de entorno."
}

confirm() {
  local answer
  read -r -p "$1 [escribe 'si' para continuar]: " answer
  [[ "$answer" == "si" ]] || die "Cancelado por el usuario."
}
