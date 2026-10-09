#!/usr/bin/env bash
#
# 80_composer_demo.sh — SO DOCENTE. Cria um Cloud Composer pequeno para mostrar
# "onde a DAG moraria em producao" e importa as mesmas DAGs.
#
# Rodar NA VESPERA: a criacao leva ~25 min. Custo fixo por hora bem maior que a VM:
# apagar logo apos a aula com ACAO=remover.
#
# A versao de Airflow do Composer deve ser compativel com a da VM (AIRFLOW_VERSION).
# Liste as imagens disponiveis com:
#   gcloud composer environments list-image-versions --location=us-central1
#
# Uso:
#   COMPOSER_IMAGE=composer-3-airflow-X.Y.Z bash gcloud/80_composer_demo.sh
#   ACAO=remover bash gcloud/80_composer_demo.sh

source "$(dirname "$0")/config.sh"
exige_projeto
ACAO="${ACAO:-criar}"
COMPOSER_ENV="${COMPOSER_ENV:-composer-demo-mlops}"

if [[ "${ACAO}" == "remover" ]]; then
  gcloud composer environments delete "${COMPOSER_ENV}" --location="${REGION}" --project="${PROJECT_ID}" --quiet
  exit 0
fi

[[ -n "${COMPOSER_IMAGE:-}" ]] || erro "Defina COMPOSER_IMAGE (veja list-image-versions no cabecalho)."

gcloud services enable composer.googleapis.com --project="${PROJECT_ID}"

if ! gcloud composer environments describe "${COMPOSER_ENV}" --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud composer environments create "${COMPOSER_ENV}" \
    --location="${REGION}" --project="${PROJECT_ID}" \
    --image-version="${COMPOSER_IMAGE}" --environment-size=small \
    --service-account="${SA_EMAIL}" \
    --env-variables="AIRFLOW_VAR_PROJETO=${PROJECT_ID},AIRFLOW_VAR_REGIAO=${REGION},AIRFLOW_VAR_BUCKET=${BUCKET},AIRFLOW_VAR_GOLD_TABLE=${GOLD_TABLE}"
fi

# scikit-learn 1.6 (casa com o container de serving). Outra atualizacao de ~10-20 min.
gcloud composer environments update "${COMPOSER_ENV}" \
  --location="${REGION}" --project="${PROJECT_ID}" \
  --update-pypi-packages-from-file="${AIRFLOW_DIR}/requirements.txt" || warn "Pacotes ja aplicados ou sem mudanca."

gcloud composer environments storage dags import \
  --environment="${COMPOSER_ENV}" --location="${REGION}" --project="${PROJECT_ID}" \
  --source="${AIRFLOW_DIR}/dags/"
ok "DAGs importadas no Composer ${COMPOSER_ENV}."
