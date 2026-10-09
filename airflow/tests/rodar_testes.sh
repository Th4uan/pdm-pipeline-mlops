#!/usr/bin/env bash
#
# rodar_testes.sh — executa as DAGs de ponta a ponta SEM GCP (nada e' criado na nuvem).
#
# Cria um venv com Airflow 3.1.0 + provider Google + scikit-learn 1.6.1 e roda o pytest.
# O GCP e' simulado em tests/fake_gcp.py: BigQuery (DuckDB), Storage (disco) e Vertex AI
# (Experiments, Registry, Endpoints com predicao real do model.joblib).
#
# Uso (a partir de pdm-pipeline-mlops/airflow):
#   bash tests/rodar_testes.sh
#
# Requer Python 3.12. Com uv instalado, a criacao do venv leva ~1 min; com pip, alguns minutos.

set -euo pipefail
cd "$(dirname "$0")/.."

AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.1.0}"
PY="${PY:-python3.12}"
VENV=".venv-testes"
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"

if [[ ! -x "${VENV}/bin/python" ]]; then
  if command -v uv >/dev/null 2>&1; then
    uv venv -q -p 3.12 "${VENV}"
    uv pip install -q -p "${VENV}/bin/python" "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-google db-dtypes --constraint "${CONSTRAINTS}"
    # scikit-learn fora das constraints: precisa casar com o container sklearn-cpu.1-6
    uv pip install -q -p "${VENV}/bin/python" "apache-airflow==${AIRFLOW_VERSION}" "scikit-learn==1.6.1" duckdb sqlglot pytest
  else
    "${PY}" -m venv "${VENV}"
    "${VENV}/bin/pip" install -q "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-google db-dtypes --constraint "${CONSTRAINTS}"
    "${VENV}/bin/pip" install -q "apache-airflow==${AIRFLOW_VERSION}" "scikit-learn==1.6.1" duckdb sqlglot pytest
  fi
fi

PYTHONDONTWRITEBYTECODE=1 "${VENV}/bin/python" -m pytest tests -q -p no:cacheprovider "$@"
