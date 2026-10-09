#!/usr/bin/env bash
#
# startup-script.sh — roda DENTRO da VM (como root) a cada boot.
#
# Instala Docker, baixa o codigo do Airflow do bucket, sincroniza as DAGs e sobe o
# Docker Compose. Cada fase e' idempotente. O progresso vai para:
#   - /var/log/airflow-setup.log (e para a porta serial, lida por 40_status.sh);
#   - o guest attribute airflow/status, lido pelo Cloud Shell em 10_airflow_vm.sh.
#
# Configuracao recebida pelos metadados da instancia: bucket, projeto, regiao,
# gold-table, airflow-image-base, airflow-image-pronta.

set -euo pipefail
exec > >(tee -a /var/log/airflow-setup.log) 2>&1

META="http://metadata.google.internal/computeMetadata/v1/instance"
meta()   { curl -sf -H "Metadata-Flavor: Google" "${META}/attributes/$1" || true; }
status() {
  curl -sf -X PUT -H "Metadata-Flavor: Google" --data "$1" \
    "${META}/guest-attributes/airflow/status" >/dev/null || true
  echo "[airflow-setup] $(date -Is) status=$1"
}
FASE="inicio"
trap 'status "erro:${FASE}"' ERR

BUCKET="$(meta bucket)"
PROJETO="$(meta projeto)"
REGIAO="$(meta regiao)"
GOLD_TABLE="$(meta gold-table)"
IMAGEM_BASE="$(meta airflow-image-base)"
IMAGEM_PRONTA="$(meta airflow-image-pronta)"

SRC=/opt/airflow-src
DAGS=/opt/airflow-dags
LOGS=/opt/airflow-logs

# ---------------------------------------------------------------------------
FASE="instalando-docker"; status "${FASE}"
if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
fi
systemctl enable --now docker

# ---------------------------------------------------------------------------
FASE="baixando-codigo"; status "${FASE}"
mkdir -p "${SRC}" "${DAGS}" "${LOGS}"
gcloud storage rsync --recursive --delete-unmatched-destination-objects \
  "gs://${BUCKET}/airflow/src" "${SRC}"
# O usuario do container (uid 50000, grupo 0) precisa escrever os logs.
chown -R 50000:0 "${LOGS}"

# .env gerado uma unica vez: o segredo JWT precisa sobreviver a reboots.
if [[ ! -f "${SRC}/.env.gerado" ]]; then
  JWT="$(head -c 32 /dev/urandom | base64 | tr -d '/+=' )"
  cat > "${SRC}/.env.gerado" <<EOF
AIRFLOW__API_AUTH__JWT_SECRET=${JWT}
EOF
fi
cat > "${SRC}/.env" <<EOF
AIRFLOW_UID=50000
AIRFLOW_IMAGE_BASE=${IMAGEM_BASE}
AIRFLOW_IMAGE_NAME=${IMAGEM_PRONTA:-airflow-mlops:local}
GOOGLE_CLOUD_PROJECT=${PROJETO}
AIRFLOW_VAR_PROJETO=${PROJETO}
AIRFLOW_VAR_REGIAO=${REGIAO}
AIRFLOW_VAR_BUCKET=${BUCKET}
AIRFLOW_VAR_GOLD_TABLE=${GOLD_TABLE}
# Airflow 3 nao cria conexoes padrao: a dos operadores Google usa as credenciais da VM (ADC).
AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT={"conn_type": "google_cloud_platform", "extra": {"project": "${PROJETO}"}}
$(cat "${SRC}/.env.gerado")
EOF

# ---------------------------------------------------------------------------
FASE="sincronizando-dags"; status "${FASE}"
cat > /usr/local/bin/airflow-dags-sync <<EOF
#!/usr/bin/env bash
# Espelha gs://${BUCKET}/airflow/dags em ${DAGS} (mesmo modelo do Cloud Composer).
exec gcloud storage rsync --recursive --delete-unmatched-destination-objects \\
  --exclude='.*__pycache__.*' "gs://${BUCKET}/airflow/dags" "${DAGS}"
EOF
chmod +x /usr/local/bin/airflow-dags-sync

cat > /etc/systemd/system/airflow-dags-sync.service <<'EOF'
[Unit]
Description=Sincroniza as DAGs do bucket para o Airflow
[Service]
Type=oneshot
ExecStart=/usr/local/bin/airflow-dags-sync
EOF
cat > /etc/systemd/system/airflow-dags-sync.timer <<'EOF'
[Unit]
Description=Sincroniza as DAGs a cada 30 segundos
[Timer]
OnBootSec=30s
OnUnitActiveSec=30s
AccuracySec=1s
[Install]
WantedBy=timers.target
EOF
systemctl daemon-reload
/usr/local/bin/airflow-dags-sync || true   # bucket ainda sem DAGs nao e' erro
systemctl enable --now airflow-dags-sync.timer

# ---------------------------------------------------------------------------
FASE="construindo-imagem"; status "${FASE}"
cd "${SRC}"
if [[ -n "${IMAGEM_PRONTA}" ]]; then
  docker pull "${IMAGEM_PRONTA}"
else
  docker compose build
fi

# ---------------------------------------------------------------------------
FASE="iniciando-airflow"; status "${FASE}"
# --pull missing: baixa o postgres na primeira vez; a imagem do Airflow ja existe localmente.
docker compose up -d --no-build --pull missing

for _ in $(seq 1 60); do
  if curl -sf "http://localhost:8080/api/v2/version" >/dev/null; then
    status "pronto"
    exit 0
  fi
  sleep 5
done
FASE="iniciando-airflow-healthcheck"
false
