#!/usr/bin/env bash
#
# 00_setup.sh — pre-condicoes do projeto para a aula de pipeline com Airflow.
#
# O que faz, de forma IDEMPOTENTE:
#   1. confere projeto e faturamento;
#   2. habilita as APIs (compute, iap, aiplatform, bigquery, storage, logging);
#   3. cria o bucket da aula ${PROJECT_ID}-mlops-aula, se faltar;
#   4. garante a camada gold da aula anterior (aula_pdm.imoveis_gold) via seed_gold.sh,
#      que so reconstroi se a tabela estiver faltando ou invalida;
#   5. mostra a quota de CPU da regiao e avisa sobre recursos esquecidos da aula anterior.
#
# Uso (Cloud Shell, a partir da raiz do repositorio pdm-pipeline-mlops):
#   bash gcloud/00_setup.sh

source "$(dirname "$0")/config.sh"
exige_projeto

info "Configuracao"
printf '  Projeto ...... %s\n' "${PROJECT_ID}"
printf '  Regiao ....... %s\n' "${REGION}"
printf '  Bucket ....... gs://%s\n' "${BUCKET}"
printf '  Gold ......... %s\n' "${GOLD_TABLE}"

# ---------------------------------------------------------------------------
info "1/5 Faturamento"
BILLING="$(gcloud billing projects describe "${PROJECT_ID}" --format='value(billingEnabled)' 2>/dev/null || true)"
if [[ "${BILLING}" == "True" ]]; then
  ok "Faturamento ativo."
else
  warn "Nao foi possivel confirmar o faturamento (resultado: '${BILLING:-vazio}'). Sem billing a API do Vertex AI nao habilita."
fi

# ---------------------------------------------------------------------------
info "2/5 APIs (a primeira habilitacao do Compute leva ~1 min)"
gcloud services enable \
  compute.googleapis.com \
  iap.googleapis.com \
  aiplatform.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  logging.googleapis.com \
  --project="${PROJECT_ID}"
ok "APIs habilitadas."

# ---------------------------------------------------------------------------
info "3/5 Bucket da aula"
if gcloud storage buckets describe "gs://${BUCKET}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  ok "gs://${BUCKET} ja existe."
else
  gcloud storage buckets create "gs://${BUCKET}" \
    --project="${PROJECT_ID}" --location="${REGION}" \
    --uniform-bucket-level-access --public-access-prevention
  ok "gs://${BUCKET} criado em ${REGION}."
fi

# ---------------------------------------------------------------------------
info "4/5 Camada gold (a mesma da aula anterior)"
PROJECT_ID="${PROJECT_ID}" REGION="${REGION}" DATASET="${DATASET}" GOLD="${GOLD}" \
  bash "${GCLOUD_DIR}/seed_gold.sh"

# ---------------------------------------------------------------------------
info "5/5 Quota e recursos esquecidos"
python3 - "${PROJECT_ID}" "${REGION}" <<'EOF' || warn "Nao foi possivel ler a quota de CPU."
import json, subprocess, sys
proj, reg = sys.argv[1], sys.argv[2]
out = subprocess.run(["gcloud", "compute", "regions", "describe", reg, "--project", proj, "--format=json"],
                     capture_output=True, text=True, check=True).stdout
for q in json.loads(out).get("quotas", []):
    if q["metric"] in ("CPUS", "SSD_TOTAL_GB"):
        print(f"  {q['metric']:<13} uso {q['usage']:.0f} de {q['limit']:.0f}")
EOF
printf '  A VM do Airflow usa 4 vCPU (e2-standard-4). Com quota de 8 ou menos: VM_TYPE=e2-standard-2.\n'

ENDPOINTS="$(gcloud ai endpoints list --region="${REGION}" --project="${PROJECT_ID}" \
  --format='value(displayName)' 2>/dev/null || true)"
if [[ -n "${ENDPOINTS}" ]]; then
  warn "Ha endpoints ligados em ${REGION} (consomem credito por hora):"
  printf '        %s\n' ${ENDPOINTS}
  warn "Se forem da aula anterior, rode: bash gcloud/90_teardown_ml.sh"
else
  ok "Nenhum endpoint ligado."
fi

info "Pronto. Proximo passo: bash gcloud/10_airflow_vm.sh"
