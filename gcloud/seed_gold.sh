#!/usr/bin/env bash
#
# seed_gold.sh — garante a camada gold da aula no BigQuery.
#
# Para quem NAO tem a tabela gold, ou tem uma gold mal formatada (sem as colunas
# que o notebook usa, ou praticamente vazia). O script VALIDA primeiro e so
# reconstroi se precisar — rodar com a gold boa nao muda nada.
#
# O que faz, de forma IDEMPOTENTE (pode rodar quantas vezes quiser):
#   1. valida a gold `${DATASET}.${GOLD}` (colunas do contrato + volume minimo);
#   2. se estiver faltando/invalida (ou com --force), reconstroi:
#      a. cria o dataset, se faltar;
#      b. baixa a amostra de anuncios do repo das aulas anteriores;
#      c. cria a tabela Bronze `${BRONZE}` e recarrega os JSONL nela;
#      d. materializa a gold com dedup por imovel + filtros de sanidade.
#
# Uso no Cloud Shell do SEU projeto:
#   cd pdm-pipeline-mlops   (copiado da aula anterior, sem alteracoes de logica)
#   bash gcloud/seed_gold.sh
#
#   # reconstruir mesmo que a gold ja exista e esteja valida:
#   bash gcloud/seed_gold.sh --force
#
#   # apontando para outro projeto/dataset:
#   PROJECT_ID=meu-projeto DATASET=aula_pdm bash gcloud/seed_gold.sh
#
# Variaveis de ambiente aceitas (todas opcionais):
#   PROJECT_ID   ID do projeto. Se ausente, usa `gcloud config get-value project`.
#   REGION       Regiao do dataset, se ele precisar ser criado (padrao us-central1).
#   DATASET      Dataset BigQuery da disciplina (padrao aula_pdm).
#   GOLD         Nome da tabela gold (padrao imoveis_gold).
#   BRONZE       Nome da tabela bronze de anuncios crus (padrao anuncios).
#   FORCE        "true" reconstroi mesmo com a gold valida. Igual a --force.
#   SAMPLE_URL   URL do zip de amostra dos anuncios.
#   MIN_LINHAS   Volume minimo para considerar a gold valida (padrao 500).
#
# NAO toca em buckets, em modelos, em endpoints nem em datasets de outras aulas.
# Escreve APENAS em ${DATASET}: as tabelas ${BRONZE} e ${GOLD}.

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
REGION="${REGION:-us-central1}"

DATASET="${DATASET:-aula_pdm}"
GOLD="${GOLD:-imoveis_gold}"
BRONZE="${BRONZE:-anuncios}"

FORCE="${FORCE:-false}"
SAMPLE_URL="${SAMPLE_URL:-https://raw.githubusercontent.com/robertogyn19/aula-pdm-pubsub/main/dados/anuncios.zip}"
MIN_LINHAS="${MIN_LINHAS:-500}"

# ---------------------------------------------------------------------------
# Utilitarios
# ---------------------------------------------------------------------------
info()  { printf '\n[ %s ] %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()    { printf '  OK   %s\n' "$*"; }
warn()  { printf '  AVISO %s\n' "$*" >&2; }
erro()  { printf '  ERRO %s\n' "$*" >&2; exit 1; }

# Roda uma query e devolve o primeiro valor da primeira linha de dados.
# String vazia se a query falhar (tabela ausente, coluna faltando, sem permissao).
bq_valor() {
  bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --format=csv --quiet "$1" 2>/dev/null \
    | awk 'NR==2 { print; exit }' || true
}

# Roda uma query descartando a saida; devolve o codigo de saida do bq.
bq_exec() {
  bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --quiet "$1" >/dev/null
}

# ---------------------------------------------------------------------------
# Argumentos
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "--force" ]]; then
  FORCE=true
elif [[ -n "${1:-}" ]]; then
  warn "Argumento desconhecido: '${1}'. O unico aceito e '--force'."
fi

command -v gcloud >/dev/null 2>&1 || erro "gcloud nao encontrado no PATH. No Cloud Shell ele ja vem instalado."
command -v bq     >/dev/null 2>&1 || erro "bq nao encontrado no PATH. Ele vem junto com o Google Cloud SDK."
command -v curl   >/dev/null 2>&1 || erro "curl nao encontrado no PATH."

[[ -n "${PROJECT_ID}" ]] || erro "PROJECT_ID vazio. Rode 'gcloud config set project SEU_PROJECT_ID' ou PROJECT_ID=seu-projeto bash gcloud/seed_gold.sh"
[[ "${PROJECT_ID}" != "SEU_PROJECT_ID" ]] || erro "Troque SEU_PROJECT_ID pelo ID real do seu projeto."

info "Configuracao"
printf '  Projeto ............ %s\n' "${PROJECT_ID}"
printf '  Regiao ............. %s (usada so se o dataset precisar ser criado)\n' "${REGION}"
printf '  Dataset ............ %s:%s\n' "${PROJECT_ID}" "${DATASET}"
printf '  Tabela bronze ...... %s.%s\n' "${DATASET}" "${BRONZE}"
printf '  Tabela gold ........ %s.%s\n' "${DATASET}" "${GOLD}"
printf '  Minimo de linhas ... %s\n' "${MIN_LINHAS}"
printf '  Reconstruir sempre . %s\n' "${FORCE}"

# ---------------------------------------------------------------------------
# 1. Validacao da gold
# ---------------------------------------------------------------------------
info "1/3 Validando a gold existente"

GOLD_VALIDA=false
GOLD_LINHAS=0

# Se a tabela nao existir, ou faltar qualquer coluna do contrato, esta query falha.
if bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --quiet \
     "SELECT preco, area_util, area_total, quartos, banheiros, garagens FROM \`${DATASET}.${GOLD}\` LIMIT 1" \
     >/dev/null 2>&1; then
  ok "Tabela ${DATASET}.${GOLD} existe e tem as colunas do contrato."
  GOLD_LINHAS="$(bq_valor "SELECT COUNT(*) FROM \`${DATASET}.${GOLD}\`")"
  GOLD_LINHAS="${GOLD_LINHAS:-0}"
  printf '  Linhas ............. %s\n' "${GOLD_LINHAS}"
  if [[ "${GOLD_LINHAS}" -ge "${MIN_LINHAS}" ]] 2>/dev/null; then
    GOLD_VALIDA=true
  else
    warn "Volume abaixo do minimo (${GOLD_LINHAS} < ${MIN_LINHAS}): a gold sera reconstruida."
  fi
else
  warn "Tabela ${DATASET}.${GOLD} ausente ou sem as colunas do contrato: sera reconstruida."
fi

if [[ "${GOLD_VALIDA}" == "true" && "${FORCE}" != "true" ]]; then
  info "Nada a fazer"
  printf '  gold ja existe e esta valida (%s linhas); nada a fazer. Use --force para reconstruir.\n' "${GOLD_LINHAS}"
  exit 0
fi

if [[ "${GOLD_VALIDA}" == "true" ]]; then
  warn "FORCE=true: a gold valida (${GOLD_LINHAS} linhas) sera RECONSTRUIDA do zero."
fi

# ---------------------------------------------------------------------------
# 2. Reconstrucao — dataset, amostra e bronze
# ---------------------------------------------------------------------------
info "2/3 Reconstruindo a partir da amostra do repo da disciplina"

# 2a. Dataset (mk --force nao falha se ja existir).
bq --project_id="${PROJECT_ID}" --location="${REGION}" mk --dataset --force \
  --description="Dados da disciplina PDM (bronze + camada gold)" \
  "${PROJECT_ID}:${DATASET}" >/dev/null 2>&1 || true
ok "Dataset ${PROJECT_ID}:${DATASET} disponivel."

# 2b. Amostra de anuncios num diretorio temporario, removido ao sair.
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

printf '  Baixando a amostra: %s\n' "${SAMPLE_URL}"
curl -sL -o "${TMP}/anuncios.zip" "${SAMPLE_URL}" \
  || erro "Falha ao baixar a amostra. Confira a conexao ou defina SAMPLE_URL."
[[ -s "${TMP}/anuncios.zip" ]] || erro "O arquivo baixado esta vazio. Confira SAMPLE_URL."

mkdir -p "${TMP}/extract"
if command -v unzip >/dev/null 2>&1; then
  unzip -oq "${TMP}/anuncios.zip" -d "${TMP}/extract"
else
  # Cloud Shell tem unzip, mas o fallback evita depender disso.
  python3 -c "import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])" \
    "${TMP}/anuncios.zip" "${TMP}/extract" \
    || erro "Nao foi possivel descompactar a amostra (sem unzip e sem python3)."
fi

# Os JSONL ficam fora do diretorio de extracao para o find nao se morder.
ALL="${TMP}/all_anuncios.jsonl"
: > "${ALL}"
find "${TMP}/extract" -type f -name '*.jsonl' -print0 | xargs -0 -r cat >> "${ALL}"
if [[ ! -s "${ALL}" ]]; then
  # fallback: alguns pacotes trazem a extensao .json mesmo sendo newline-delimited
  find "${TMP}/extract" -type f -name '*.json' -print0 | xargs -0 -r cat >> "${ALL}"
fi
[[ -s "${ALL}" ]] || erro "Nenhum arquivo .jsonl encontrado dentro do zip da amostra."

LINHAS_JSONL="$(wc -l < "${ALL}" | tr -d ' ')"
ok "Amostra pronta: ${LINHAS_JSONL} linhas em all_anuncios.jsonl."

# 2c. Tabela bronze com o schema dos anuncios crus.
#     Particionada por dia e clusterizada por cidade/estado, como na aula de ingestao.
bq_exec "$(cat <<EOF
CREATE TABLE IF NOT EXISTS \`${DATASET}.${BRONZE}\` (
  id INT64,
  titulo STRING,
  ativo BOOL,
  aceita_troca BOOL,
  pet_friendly BOOL,
  descricao STRING,
  area_total FLOAT64,
  area_util FLOAT64,
  categoria STRING,
  preco FLOAT64,
  preco_fmt STRING,
  preco_iptu STRING,
  imagens ARRAY<STRING>,
  transacao STRING,
  suites INT64,
  quartos INT64,
  banheiros INT64,
  garagens INT64,
  latitude FLOAT64,
  longitude FLOAT64,
  rua STRING,
  bairro STRING,
  cidade STRING,
  estado STRING,
  cep STRING,
  data_atualizacao DATETIME
)
PARTITION BY DATE(data_atualizacao)
CLUSTER BY cidade, estado;
EOF
)" || erro "Falha ao criar a tabela bronze ${DATASET}.${BRONZE}."
ok "Tabela bronze ${DATASET}.${BRONZE} pronta."

# Esvaziar antes de carregar: e' isto que impede duplicar os anuncios ao rodar 2x.
bq_exec "TRUNCATE TABLE \`${DATASET}.${BRONZE}\`;" \
  || erro "Falha ao esvaziar ${DATASET}.${BRONZE} antes da recarga."
ok "Tabela bronze esvaziada (recarga nao duplica linhas)."

# 2d. Carga. Sem --schema de proposito: o bq usa o schema da tabela, que ja
#     declara `imagens` como ARRAY<STRING> (campo REPEATED).
printf '  Carregando %s linhas em %s.%s ...\n' "${LINHAS_JSONL}" "${DATASET}" "${BRONZE}"
bq --project_id="${PROJECT_ID}" load \
  --source_format=NEWLINE_DELIMITED_JSON \
  "${DATASET}.${BRONZE}" \
  "${ALL}" \
  || erro "Falha ao carregar os anuncios em ${DATASET}.${BRONZE}."
ok "Carga concluida."

# ---------------------------------------------------------------------------
# 3. Materializacao da gold
# ---------------------------------------------------------------------------
info "3/3 Materializando a gold"

# Dedup por imovel (ultima atualizacao vence), so imoveis ativos a venda, e
# filtros de sanidade em preco, area e quartos. CREATE OR REPLACE e' idempotente.
bq_exec "$(cat <<EOF
CREATE OR REPLACE TABLE \`${DATASET}.${GOLD}\` AS
WITH dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY data_atualizacao DESC) AS rn
    FROM \`${DATASET}.${BRONZE}\`
    WHERE ativo IS TRUE AND transacao = 'SELL'
  )
  WHERE rn = 1
)
SELECT
  id, preco, area_util, area_total, quartos, banheiros, garagens, suites,
  bairro, cidade, estado
FROM dedup
WHERE preco BETWEEN 10000 AND 50000000
  AND area_util > 0
  AND quartos BETWEEN 0 AND 20;
EOF
)" || erro "Falha ao materializar ${DATASET}.${GOLD}."
ok "Tabela ${DATASET}.${GOLD} recriada."

# ---------------------------------------------------------------------------
# Resumo
# ---------------------------------------------------------------------------
info "Resumo da gold"
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false \
  "SELECT COUNT(*) AS linhas, ROUND(AVG(preco)) AS preco_medio FROM \`${DATASET}.${GOLD}\`" \
  || warn "Nao foi possivel ler o resumo da gold."

cat <<EOF

  Projeto ............. ${PROJECT_ID}
  Dataset ............. ${PROJECT_ID}:${DATASET}
  Bronze .............. ${DATASET}.${BRONZE}
  Gold ................ ${DATASET}.${GOLD}
  GOLD_TABLE do notebook: ${PROJECT_ID}.${DATASET}.${GOLD}

  Pronto. Agora rode o notebook da aula.
EOF
