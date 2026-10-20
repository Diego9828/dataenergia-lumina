#!/usr/bin/env bash
# Fase 1 · Paso 1: habilita las APIs necesarias y crea el presupuesto con alertas.
# Costo: US$0 (las APIs y los presupuestos no cobran).
# Las alertas AVISAN por correo, no apagan nada: el corte real es infra/shutdown.sh.
source "$(dirname "$0")/_common.sh"
require_config
require_billing

log "Verificando que el proyecto $PROJECT_ID esté vinculado a la cuenta de facturación"
linked="$(gcloud billing projects describe "$PROJECT_ID" --format='value(billingAccountName)')"
[[ "$linked" == "billingAccounts/$BILLING_ACCOUNT" ]] \
  || die "El proyecto está vinculado a '$linked', no a billingAccounts/$BILLING_ACCOUNT."

log "Habilitando APIs"
gcloud services enable --project "$PROJECT_ID" \
  billingbudgets.googleapis.com \
  cloudresourcemanager.googleapis.com \
  serviceusage.googleapis.com \
  iam.googleapis.com \
  compute.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com \
  dataproc.googleapis.com \
  secretmanager.googleapis.com \
  pubsub.googleapis.com

log "Creando presupuesto $BUDGET_NAME ($BUDGET_AMOUNT ≈ US\$300) con alertas en ≈US\$50, US\$150 y US\$300"
existing="$(gcloud billing budgets list --billing-account "$BILLING_ACCOUNT" \
  --filter="displayName=$BUDGET_NAME" --format='value(name)')"
if [[ -n "$existing" ]]; then
  echo "Ya existe: $existing (no se duplica)"
else
  # Umbrales como fracción de US$300: 50/300 = 0.1667, 150/300 = 0.5, 300/300 = 1.0
  gcloud billing budgets create \
    --billing-account "$BILLING_ACCOUNT" \
    --display-name "$BUDGET_NAME" \
    --budget-amount "$BUDGET_AMOUNT" \
    --filter-projects "projects/$PROJECT_ID" \
    --threshold-rule percent=0.1667 \
    --threshold-rule percent=0.5 \
    --threshold-rule percent=1.0 \
    --threshold-rule percent=0.5,basis=forecasted-spend
fi

log "Listo. Los correos de alerta llegan a los administradores de la cuenta de facturación."
