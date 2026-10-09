#!/usr/bin/env bash
#
# 40_status.sh — diagnostico do ambiente. Primeiro comando a rodar quando algo travar.
#
# Mostra: estado da VM, fase da instalacao, ultimas linhas do log da VM, DAGs no bucket
# e recursos do Vertex AI que consomem credito.
#
# Uso:
#   bash gcloud/40_status.sh

source "$(dirname "$0")/config.sh"
exige_projeto

info "VM ${VM_NAME}"
if vm_existe; then
  gcloud compute instances describe "${VM_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}" \
    --format='value(status,machineType.basename())' | sed 's|^|  estado e maquina: |'
  printf '  instalacao: %s\n' "$(status_vm)"
  info "Ultimas linhas do log de instalacao (porta serial)"
  gcloud compute instances get-serial-port-output "${VM_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}" 2>/dev/null \
    | grep -E 'startup-script|airflow-setup' | tail -n 15 | sed 's/^/  /' || true
else
  skip "VM nao existe."
fi

info "DAGs no bucket — versao: $(versao_dags_bucket)"
gcloud storage ls --recursive "gs://${BUCKET}/airflow/dags/**" 2>/dev/null \
  | sed "s#gs://${BUCKET}/airflow/dags/#  - #" || skip "Nenhuma DAG enviada."

info "Vertex AI em ${REGION} (endpoint consome credito por hora)"
printf '  Endpoints:\n'
gcloud ai endpoints list --region="${REGION}" --project="${PROJECT_ID}" \
  --format='value(displayName,name.basename())' 2>/dev/null | sed 's/^/    /' || true
printf '  Modelos:\n'
gcloud ai models list --region="${REGION}" --project="${PROJECT_ID}" \
  --format='value(displayName,name.basename())' 2>/dev/null | sed 's/^/    /' || true
