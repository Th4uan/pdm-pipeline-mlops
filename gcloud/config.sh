# shellcheck shell=bash
#
# config.sh — contrato de nomes e utilitarios comuns. Carregado por todos os scripts:
#   source "$(dirname "$0")/config.sh"
#
# Qualquer variavel abaixo pode ser sobrescrita no ambiente, por exemplo:
#   VM_TYPE=e2-standard-2 bash gcloud/instalar_airflow.sh

set -euo pipefail

# ---------------------------------------------------------------------------
# Projeto: variavel PROJECT_ID > gcloud config > DEVSHELL_PROJECT_ID (Cloud Shell)
# ---------------------------------------------------------------------------
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
PROJECT_ID="${PROJECT_ID:-${DEVSHELL_PROJECT_ID:-}}"

REGION="${REGION:-us-central1}"
ZONE="${ZONE:-us-central1-a}"

# ---------------------------------------------------------------------------
# Dados e artefatos (os mesmos da aula anterior)
# ---------------------------------------------------------------------------
DATASET="${DATASET:-aula_pdm}"
GOLD="${GOLD:-imoveis_gold}"
GOLD_TABLE="${GOLD_TABLE:-${PROJECT_ID}.${DATASET}.${GOLD}}"
BUCKET="${BUCKET:-${PROJECT_ID}-mlops-aula}"
BUCKET_COMPARTILHADO="${PROJECT_ID}-aula-pdm"   # aulas anteriores: NUNCA tocar

MODEL_DISPLAY_NAME="rf-preco-imoveis"
ENDPOINT_DISPLAY_NAME="rf-preco-imoveis-endpoint"

# ---------------------------------------------------------------------------
# Ambiente Airflow
# ---------------------------------------------------------------------------
VM_NAME="${VM_NAME:-airflow-mlops}"
VM_TYPE="${VM_TYPE:-e2-standard-4}"      # e2-standard-2 se a quota de CPU for baixa
VM_DISK_GB="${VM_DISK_GB:-50}"           # pd-standard: nao consome a quota SSD_TOTAL_GB
VM_TAG="airflow-mlops"
NETWORK="${NETWORK:-default}"
FW_RULE="allow-iap-airflow"
IAP_RANGE="35.235.240.0/20"              # faixa de origem do Identity-Aware Proxy
USAR_NAT="${USAR_NAT:-false}"            # true: VM sem IP externo, saida via Cloud NAT
ROUTER_NAME="airflow-router"
NAT_NAME="airflow-nat"

SA_NAME="${SA_NAME:-airflow-mlops}"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Versao do Airflow: fixar no ensaio. A imagem oficial ja traz o provider Google.
AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.1.0}"
AIRFLOW_PYTHON="${AIRFLOW_PYTHON:-3.12}"
AIRFLOW_IMAGE_BASE="${AIRFLOW_IMAGE_BASE:-apache/airflow:${AIRFLOW_VERSION}-python${AIRFLOW_PYTHON}}"
# Imagem ja construida (opcional). Se preenchida, a VM faz pull em vez de build.
AIRFLOW_IMAGE_PRONTA="${AIRFLOW_IMAGE_PRONTA:-}"

UI_PORT="${UI_PORT:-8080}"

# Caminhos locais: os scripts funcionam a partir de qualquer diretorio.
GCLOUD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AULA_DIR="$(cd "${GCLOUD_DIR}/.." && pwd)"
AIRFLOW_DIR="${AULA_DIR}/airflow"

# ---------------------------------------------------------------------------
# Utilitarios
# ---------------------------------------------------------------------------
info()  { printf '\n[ %s ] %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()    { printf '  OK    %s\n' "$*"; }
skip()  { printf '  PULA  %s\n' "$*"; }
warn()  { printf '  AVISO %s\n' "$*" >&2; }
erro()  { printf '  ERRO  %s\n' "$*" >&2; exit 1; }

exige_projeto() {
  command -v gcloud >/dev/null 2>&1 || erro "gcloud nao encontrado. Rode no Cloud Shell."
  [[ -n "${PROJECT_ID}" ]] || erro "Projeto nao definido. Rode: gcloud config set project SEU_PROJECT_ID"
  [[ "${PROJECT_ID}" != "SEU_PROJECT_ID" ]] || erro "Troque SEU_PROJECT_ID pelo ID real do seu projeto."
  [[ "${BUCKET}" != "${BUCKET_COMPARTILHADO}" ]] || erro "BUCKET nao pode ser o bucket compartilhado ${BUCKET_COMPARTILHADO}."
}

vm_existe() {
  gcloud compute instances describe "${VM_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}" >/dev/null 2>&1
}

# Le o progresso publicado pelo startup script da VM (guest attribute airflow/status).
status_vm() {
  gcloud compute instances get-guest-attributes "${VM_NAME}" \
    --zone="${ZONE}" --project="${PROJECT_ID}" \
    --query-path=airflow/status --format='value(value)' 2>/dev/null || true
}

# Qual versao da DAG esta no bucket: GABARITO (completa) ou ALUNO (com TODOs pendentes).
versao_dags_bucket() {
  local dag sql
  dag="$(gcloud storage cat "gs://${BUCKET}/airflow/dags/pipeline_preco_imoveis.py" 2>/dev/null || true)"
  sql="$(gcloud storage cat "gs://${BUCKET}/airflow/dags/sql/preparar_dataset.sql" 2>/dev/null || true)"
  if [[ -z "${dag}" ]]; then
    echo "nenhuma"
  elif grep -q "(GABARITO)" <<<"${dag}"; then
    echo "GABARITO (fluxo completo)"
  else
    local pendentes=()
    grep -q "TODO 1" <<<"${sql}" && pendentes+=("1")
    grep -q "# TODO 2" <<<"${dag}" && pendentes+=("2")
    grep -q "# TODO 3" <<<"${dag}" && pendentes+=("3")
    if (( ${#pendentes[@]} )); then
      echo "ALUNO (TODOs pendentes: ${pendentes[*]})"
    else
      echo "ALUNO (TODOs concluidos)"
    fi
  fi
}
