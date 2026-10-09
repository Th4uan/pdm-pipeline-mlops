#!/usr/bin/env bash
#
# 30_abrir_ui.sh — abre a interface do Airflow pelo Cloud Shell.
#
# Cria um tunel IAP da porta 8080 da VM para localhost:8080 do Cloud Shell. Nao usa
# SSH (evita gerar chave no primeiro uso) e nao expoe a VM a internet.
# Com o tunel aberto: Web Preview (icone no topo do Cloud Shell) > Preview on port 8080.
#
# O tunel fica em primeiro plano: deixe esta aba rodando e use outra aba do Cloud Shell
# para os demais comandos. Se a sessao cair, rode de novo.
#
# Uso:
#   bash gcloud/30_abrir_ui.sh
#   UI_PORT=8081 bash gcloud/30_abrir_ui.sh   # se a 8080 do Cloud Shell estiver ocupada

source "$(dirname "$0")/config.sh"
exige_projeto

vm_existe || erro "VM ${VM_NAME} nao existe. Rode: bash gcloud/instalar_airflow.sh"
ESTADO="$(status_vm)"
[[ "${ESTADO}" == "pronto" ]] || warn "Status da VM: '${ESTADO:-desconhecido}'. A UI pode ainda nao responder."

cat <<EOF

  Tunel: VM ${VM_NAME}:8080  ->  Cloud Shell localhost:${UI_PORT}

  1. Clique em Web Preview (icone de olho/tela no topo do Cloud Shell).
  2. "Preview on port ${UI_PORT}" (ou "Change port" e digite ${UI_PORT}).
  3. A UI do Airflow abre em outra aba, sem login.

  Ctrl+C encerra o tunel (o Airflow continua rodando na VM).

EOF

exec gcloud compute start-iap-tunnel "${VM_NAME}" 8080 \
  --local-host-port="localhost:${UI_PORT}" \
  --zone="${ZONE}" --project="${PROJECT_ID}"
