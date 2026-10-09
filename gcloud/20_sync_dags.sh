#!/usr/bin/env bash
#
# 20_sync_dags.sh — envia as DAGs do Cloud Shell para o Airflow.
#
# Fluxo (o mesmo do Cloud Composer):
#   airflow/dags/  --(este script)-->  gs://${BUCKET}/airflow/dags/  --(VM, a cada 30 s)-->  Airflow
#
# Antes de enviar, confere a sintaxe de cada .py: um erro aqui economiza a ida e volta
# ate a tela "DAG Import Errors". Apagar um arquivo local remove a DAG do Airflow.
#
# Uso:
#   bash gcloud/20_sync_dags.sh                 # aluno: envia airflow/dags como esta
#   GABARITO=true bash gcloud/20_sync_dags.sh   # docente: envia a DAG completa (gabarito)

source "$(dirname "$0")/config.sh"
exige_projeto
GABARITO="${GABARITO:-false}"

ORIGEM="${AIRFLOW_DIR}/dags"
if [[ "${GABARITO}" == "true" ]]; then
  # Copia temporaria: dags/ + gabarito/ por cima. O repositorio local nao e' alterado.
  ORIGEM="$(mktemp -d)"
  trap 'rm -rf "${ORIGEM}"' EXIT
  cp -R "${AIRFLOW_DIR}/dags/." "${ORIGEM}/"
  cp -R "${AIRFLOW_DIR}/gabarito/." "${ORIGEM}/"
  warn "GABARITO=true: enviando a DAG completa (modo docente)."
fi

info "Conferindo a sintaxe das DAGs"
python3 - "${ORIGEM}" <<'EOF'
import ast, pathlib, sys
erros = 0
for f in sorted(pathlib.Path(sys.argv[1]).rglob("*.py")):
    try:
        ast.parse(f.read_text(encoding="utf-8"), str(f))
    except SyntaxError as e:
        erros += 1
        print(f"  ERRO  {f.name}:{e.lineno}: {e.msg}")
sys.exit(1 if erros else 0)
EOF
ok "Sintaxe ok."

info "Enviando airflow/dags -> gs://${BUCKET}/airflow/dags"
gcloud storage rsync --recursive --delete-unmatched-destination-objects \
  --exclude='.*__pycache__.*' \
  "${ORIGEM}" "gs://${BUCKET}/airflow/dags"

gcloud storage ls --recursive "gs://${BUCKET}/airflow/dags/**" 2>/dev/null \
  | sed "s#gs://${BUCKET}/airflow/dags/#  - #"
ok "Enviado. A DAG aparece na UI em ate ~1 min (sincronizacao + leitura do Airflow)."
