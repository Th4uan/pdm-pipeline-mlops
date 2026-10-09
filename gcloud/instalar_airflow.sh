#!/usr/bin/env bash
#
# instalar_airflow.sh — instalacao completa com um comando, no Cloud Shell.
#
#   00_setup.sh      APIs, bucket, camada gold (da aula anterior)
#   10_airflow_vm.sh service account, firewall IAP, VM com Airflow (envia as DAGs)
#
# Idempotente: se o Cloud Shell cair, rode de novo e ele continua de onde parou.
#
# Uso (a partir da raiz do repositorio pdm-pipeline-mlops):
#   gcloud config set project SEU_PROJECT_ID
#   bash gcloud/instalar_airflow.sh
#
# Docente (ambiente de demonstracao, ja com a DAG completa):
#   GABARITO=true bash gcloud/instalar_airflow.sh

source "$(dirname "$0")/config.sh"
exige_projeto

bash "${GCLOUD_DIR}/00_setup.sh"
bash "${GCLOUD_DIR}/10_airflow_vm.sh"
