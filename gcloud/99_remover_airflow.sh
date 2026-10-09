#!/usr/bin/env bash
#
# 99_remover_airflow.sh — remove o ambiente Airflow (ultimo passo da aula).
#
# Remove: VM, firewall do IAP, Cloud Router/NAT (se criados) e gs://${BUCKET}/airflow/.
# Mantem: service account (custo zero; REMOVER_SA=true apaga), bucket, gold, experimentos.
#
# Antes de remover, avisa se ainda houver endpoint ligado: rode a DAG teardown_mlops
# (ou bash gcloud/90_teardown_ml.sh) primeiro.
#
# Uso:
#   bash gcloud/99_remover_airflow.sh

source "$(dirname "$0")/config.sh"
exige_projeto
REMOVER_SA="${REMOVER_SA:-false}"

info "Conferindo recursos de ML ainda ligados"
ENDPOINTS="$(gcloud ai endpoints list --region="${REGION}" --project="${PROJECT_ID}" \
  --filter="displayName=${ENDPOINT_DISPLAY_NAME}" --format='value(name)' 2>/dev/null || true)"
if [[ -n "${ENDPOINTS}" ]]; then
  warn "O endpoint ${ENDPOINT_DISPLAY_NAME} ainda existe e consome credito por hora."
  warn "Rode antes a DAG teardown_mlops ou: bash gcloud/90_teardown_ml.sh"
  read -r -p "  Remover o Airflow mesmo assim? [s/N] " RESP
  [[ "${RESP}" =~ ^[sS]$ ]] || erro "Cancelado."
else
  ok "Nenhum endpoint da aula ligado."
fi

info "Removendo a VM ${VM_NAME}"
if vm_existe; then
  gcloud compute instances delete "${VM_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}" --quiet
  ok "VM removida."
else
  skip "VM nao existe."
fi

info "Removendo rede auxiliar"
if gcloud compute firewall-rules describe "${FW_RULE}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud compute firewall-rules delete "${FW_RULE}" --project="${PROJECT_ID}" --quiet
  ok "Firewall ${FW_RULE} removido."
else
  skip "Firewall ${FW_RULE} nao existe."
fi
if gcloud compute routers describe "${ROUTER_NAME}" --region="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud compute routers delete "${ROUTER_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --quiet
  ok "Cloud Router/NAT removidos."
fi

info "Removendo gs://${BUCKET}/airflow/"
if gcloud storage ls "gs://${BUCKET}/airflow/" >/dev/null 2>&1; then
  gcloud storage rm --recursive "gs://${BUCKET}/airflow/"
  ok "Codigo e DAGs removidos do bucket."
else
  skip "Nada em gs://${BUCKET}/airflow/."
fi

if [[ "${REMOVER_SA}" == "true" ]]; then
  gcloud iam service-accounts delete "${SA_EMAIL}" --project="${PROJECT_ID}" --quiet
  ok "Service account removida."
fi

cat <<EOF

  Varredura final (confira no console, regiao ${REGION}):
    Compute Engine > VM instances ........ sem ${VM_NAME}
    Vertex AI > Online prediction ........ sem ${ENDPOINT_DISPLAY_NAME}
    Vertex AI > Model Registry ........... sem ${MODEL_DISPLAY_NAME}
  Devem CONTINUAR existindo: gs://${BUCKET_COMPARTILHADO} e ${DATASET}.${GOLD}.
EOF
