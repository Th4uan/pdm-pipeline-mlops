#!/usr/bin/env bash
#
# 90_teardown_ml.sh — encerra os recursos de ML da aula SEM depender do Airflow.
#
# Mesma ordem da DAG teardown_mlops (do mais caro ao mais barato):
#   1. undeploy de todos os modelos do endpoint rf-preco-imoveis-endpoint e delete do endpoint;
#   2. versoes e modelo rf-preco-imoveis no Model Registry;
#   3. artefatos em gs://${BUCKET}/models/rf/;
#   4. tabelas aula_pdm.treino_* criadas pela DAG.
#
# Use quando a VM ja foi removida, a DAG de teardown falhou, ou para limpar a aula anterior.
# PROTEGIDOS (nunca tocados): gs://${PROJECT_ID}-aula-pdm e a gold aula_pdm.imoveis_gold.
#
# Uso:
#   bash gcloud/90_teardown_ml.sh
#   DRY_RUN=true bash gcloud/90_teardown_ml.sh   # so mostra o que seria removido

source "$(dirname "$0")/config.sh"
exige_projeto
DRY_RUN="${DRY_RUN:-false}"

run() {
  if [[ "${DRY_RUN}" == "true" ]]; then printf '  [DRY_RUN] %s\n' "$*"; return 0; fi
  printf '  -> %s\n' "$*"
  "$@" || warn "comando falhou (recurso ja removido?): $*"
}

# ---------------------------------------------------------------------------
info "1/4 Endpoint ${ENDPOINT_DISPLAY_NAME} (maior custo)"
ENDPOINT_IDS="$(gcloud ai endpoints list --region="${REGION}" --project="${PROJECT_ID}" \
  --filter="displayName=${ENDPOINT_DISPLAY_NAME}" --format='value(name.basename())' 2>/dev/null || true)"
if [[ -z "${ENDPOINT_IDS}" ]]; then
  skip "Nenhum endpoint."
else
  for EP in ${ENDPOINT_IDS}; do
    DEPLOYED="$(gcloud ai endpoints describe "${EP}" --region="${REGION}" --project="${PROJECT_ID}" \
      --flatten='deployedModels[]' --format='value(deployedModels.id)' 2>/dev/null || true)"
    for DM in ${DEPLOYED}; do
      run gcloud ai endpoints undeploy-model "${EP}" --region="${REGION}" --project="${PROJECT_ID}" \
        --deployed-model-id="${DM}" --quiet
    done
    run gcloud ai endpoints delete "${EP}" --region="${REGION}" --project="${PROJECT_ID}" --quiet
  done
  ok "Endpoint removido."
fi

# ---------------------------------------------------------------------------
info "2/4 Modelo ${MODEL_DISPLAY_NAME} e versoes"
MODEL_IDS="$(gcloud ai models list --region="${REGION}" --project="${PROJECT_ID}" \
  --filter="displayName=${MODEL_DISPLAY_NAME}" --format='value(name.basename())' 2>/dev/null || true)"
if [[ -z "${MODEL_IDS}" ]]; then
  skip "Nenhum modelo."
else
  for M in ${MODEL_IDS}; do
    # versoes nao-default primeiro; a default sai junto com o modelo
    VERSOES="$(gcloud ai models list-version "${M}" --region="${REGION}" --project="${PROJECT_ID}" \
      --filter='NOT versionAliases:default' --format='value(versionId)' 2>/dev/null || true)"
    for V in ${VERSOES}; do
      run gcloud ai models delete-version "${M}@${V}" --region="${REGION}" --project="${PROJECT_ID}" --quiet
    done
    run gcloud ai models delete "${M}" --region="${REGION}" --project="${PROJECT_ID}" --quiet
  done
  ok "Modelo removido."
fi

# ---------------------------------------------------------------------------
info "3/4 Artefatos gs://${BUCKET}/models/rf/"
if gcloud storage ls "gs://${BUCKET}/models/rf/" >/dev/null 2>&1; then
  run gcloud storage rm --recursive "gs://${BUCKET}/models/rf/"
  ok "Artefatos removidos."
else
  skip "Nenhum artefato."
fi

# ---------------------------------------------------------------------------
info "4/4 Tabelas ${DATASET}.treino_*"
TABELAS="$(bq --project_id="${PROJECT_ID}" ls --max_results=1000 --format=json "${DATASET}" 2>/dev/null \
  | python3 -c 'import json,sys; [print(t["tableReference"]["tableId"]) for t in json.load(sys.stdin) if t["tableReference"]["tableId"].startswith("treino_")]' 2>/dev/null || true)"
if [[ -z "${TABELAS}" ]]; then
  skip "Nenhuma tabela treino_*."
else
  for T in ${TABELAS}; do
    run bq --project_id="${PROJECT_ID}" rm -f -t "${DATASET}.${T}"
  done
  ok "Tabelas por execucao removidas (a gold ${DATASET}.${GOLD} permanece)."
fi

info "Pronto. Para remover tambem a VM do Airflow: bash gcloud/99_remover_airflow.sh"
