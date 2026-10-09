#!/usr/bin/env bash
#
# 10_airflow_vm.sh — cria o ambiente Airflow do aluno: identidade, rede e VM.
#
# O que faz, de forma IDEMPOTENTE (verifica antes de criar):
#   1. service account airflow-mlops com os papeis minimos;
#   2. rede (se faltar a default), firewall liberando so o IAP e, opcionalmente, Cloud NAT;
#   3. envia o codigo do Airflow (Dockerfile, compose, requirements) para o bucket;
#   4. cria a VM com o startup-script.sh, que instala Docker e sobe o Airflow;
#   5. espera a VM publicar o status "pronto".
#
# O trabalho pesado roda NA VM. Se o Cloud Shell cair durante a espera, rode este
# script de novo: nada e' recriado e ele so volta a acompanhar o progresso.
#
# Uso:
#   bash gcloud/10_airflow_vm.sh
#   USAR_NAT=true bash gcloud/10_airflow_vm.sh       # organizacao bloqueia IP externo
#   REINICIAR=true bash gcloud/10_airflow_vm.sh      # reaplica o codigo do Airflow na VM existente
#   GABARITO=true bash gcloud/10_airflow_vm.sh       # docente: ja sobe com a DAG completa

source "$(dirname "$0")/config.sh"
exige_projeto
REINICIAR="${REINICIAR:-false}"
ESPERA_MAX_MIN="${ESPERA_MAX_MIN:-20}"

# ---------------------------------------------------------------------------
info "1/5 Service account ${SA_EMAIL}"
if gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  ok "Service account ja existe."
else
  gcloud iam service-accounts create "${SA_NAME}" --project="${PROJECT_ID}" \
    --display-name="Airflow da aula de MLOps"
  ok "Service account criada. Aguardando propagacao..."
  sleep 15
fi

# Papeis no projeto (Vertex AI, jobs do BigQuery, logs).
for ROLE in roles/aiplatform.user roles/bigquery.jobUser roles/logging.logWriter; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" --role="${ROLE}" \
    --condition=None --quiet >/dev/null
  ok "${ROLE} (projeto)"
done

# Escrita restrita ao dataset da aula e ao bucket da aula — nunca ao bucket compartilhado.
if bq --project_id="${PROJECT_ID}" add-iam-policy-binding \
     --member="serviceAccount:${SA_EMAIL}" --role="roles/bigquery.dataEditor" \
     "${PROJECT_ID}:${DATASET}" >/dev/null 2>&1; then
  ok "roles/bigquery.dataEditor (dataset ${DATASET})"
else
  # Versoes antigas do bq nao aceitam IAM em dataset: cai para o nivel do projeto.
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" --role="roles/bigquery.dataEditor" \
    --condition=None --quiet >/dev/null
  warn "roles/bigquery.dataEditor concedido no PROJETO (bq sem suporte a IAM de dataset)."
fi

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${SA_EMAIL}" --role="roles/storage.objectAdmin" >/dev/null
ok "roles/storage.objectAdmin (gs://${BUCKET})"

# ---------------------------------------------------------------------------
info "2/5 Rede e firewall"
if gcloud compute networks describe "${NETWORK}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  ok "Rede ${NETWORK} existe."
else
  gcloud compute networks create "${NETWORK}" --project="${PROJECT_ID}" --subnet-mode=auto
  ok "Rede ${NETWORK} criada (modo auto)."
fi

if gcloud compute firewall-rules describe "${FW_RULE}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  ok "Firewall ${FW_RULE} existe."
else
  gcloud compute firewall-rules create "${FW_RULE}" --project="${PROJECT_ID}" \
    --network="${NETWORK}" --direction=INGRESS --action=allow \
    --rules=tcp:22,tcp:8080 --source-ranges="${IAP_RANGE}" --target-tags="${VM_TAG}"
  ok "Firewall ${FW_RULE}: portas 22 e 8080 apenas a partir do IAP."
fi

ADDRESS_FLAG=()
if [[ "${USAR_NAT}" == "true" ]]; then
  if ! gcloud compute routers describe "${ROUTER_NAME}" --region="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute routers create "${ROUTER_NAME}" --region="${REGION}" --network="${NETWORK}" --project="${PROJECT_ID}"
  fi
  if ! gcloud compute routers nats describe "${NAT_NAME}" --router="${ROUTER_NAME}" --region="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute routers nats create "${NAT_NAME}" --router="${ROUTER_NAME}" --region="${REGION}" \
      --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges --project="${PROJECT_ID}"
  fi
  ok "Cloud NAT pronto: a VM sai para a internet sem IP externo."
  ADDRESS_FLAG=(--no-address)
fi

# ---------------------------------------------------------------------------
info "3/5 Enviando o codigo do Airflow para gs://${BUCKET}/airflow/src"
gcloud storage rsync --recursive --delete-unmatched-destination-objects \
  --exclude='^(dags|gabarito|tests)/|.*__pycache__.*|^\.env$' \
  "${AIRFLOW_DIR}" "gs://${BUCKET}/airflow/src"
ok "Codigo enviado."

# As DAGs vao junto na primeira instalacao (depois, use 20_sync_dags.sh).
bash "${GCLOUD_DIR}/20_sync_dags.sh"

# ---------------------------------------------------------------------------
info "4/5 VM ${VM_NAME}"
if vm_existe; then
  ok "VM ja existe."
  if [[ "${REINICIAR}" == "true" ]]; then
    gcloud compute instances reset "${VM_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}" --quiet
    ok "VM reiniciada: o startup script reaplica o codigo do bucket."
    sleep 20
  fi
else
  gcloud compute instances create "${VM_NAME}" \
    --project="${PROJECT_ID}" --zone="${ZONE}" \
    --machine-type="${VM_TYPE}" \
    --image-family=debian-12 --image-project=debian-cloud \
    --boot-disk-size="${VM_DISK_GB}GB" --boot-disk-type=pd-standard \
    --network="${NETWORK}" --tags="${VM_TAG}" \
    --service-account="${SA_EMAIL}" --scopes=cloud-platform \
    --metadata-from-file=startup-script="${GCLOUD_DIR}/startup-script.sh" \
    --metadata="enable-guest-attributes=TRUE,bucket=${BUCKET},projeto=${PROJECT_ID},regiao=${REGION},gold-table=${GOLD_TABLE},airflow-image-base=${AIRFLOW_IMAGE_BASE},airflow-image-pronta=${AIRFLOW_IMAGE_PRONTA}" \
    "${ADDRESS_FLAG[@]}"
  ok "VM criada. A instalacao do Airflow roda dentro dela (~8-12 min)."
fi

# ---------------------------------------------------------------------------
info "5/5 Aguardando o Airflow (ate ${ESPERA_MAX_MIN} min; pode fechar e rodar de novo)"
FIM=$(( $(date +%s) + ESPERA_MAX_MIN * 60 ))
ULTIMO=""
while (( $(date +%s) < FIM )); do
  ATUAL="$(status_vm)"
  if [[ "${ATUAL}" != "${ULTIMO}" ]]; then
    printf '  %s  status: %s\n' "$(date +%H:%M:%S)" "${ATUAL:-iniciando-vm}"
    ULTIMO="${ATUAL}"
  fi
  case "${ATUAL}" in
    pronto)
      ok "Airflow no ar."
      info "Proximo passo: bash gcloud/30_abrir_ui.sh  (e Web Preview > porta ${UI_PORT})"
      exit 0 ;;
    erro:*)
      erro "A instalacao falhou na fase '${ATUAL#erro:}'. Diagnostico: bash gcloud/40_status.sh" ;;
  esac
  sleep 15
done
erro "Tempo esgotado sem status 'pronto'. Rode de novo para continuar esperando, ou: bash gcloud/40_status.sh"
