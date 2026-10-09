# 00 — Pré-requisitos

## Aluno

- [ ] Projeto GCP da aula anterior, com **faturamento ativo** e você como `Owner`.
- [ ] Projeto selecionado no Cloud Shell: `gcloud config set project SEU_PROJECT_ID`.
- [ ] Recursos da aula anterior **desligados**: endpoint e runtime do Colab Enterprise. Se sobrou algo: `bash gcloud/90_teardown_ml.sh`.
- [ ] A gold `aula_pdm.imoveis_gold` existe. Se não existir ou estiver inválida, o `00_setup.sh` roda o [`gcloud/seed_gold.sh`](gcloud/seed_gold.sh) da aula anterior, que a reconstrói a partir da amostra do repositório.

## Contrato de dados (o mesmo da aula anterior)

| Item | Definição |
|---|---|
| Alvo | `preco` — preço de anúncio |
| Features (ordem canônica) | `area_util, area_total, quartos, banheiros, garagens` |
| Chave do imóvel | `id` |
| Baseline | Mediana do `preco` de treino |
| Métrica | MAE em reais na validação; teste só para estimativa final |

## Pode dar problema

| Situação | O que fazer |
|---|---|
| Quota `CPUS` em `us-central1` ≤ 8 | `VM_TYPE=e2-standard-2 bash gcloud/instalar_airflow.sh` |
| Organização bloqueia IP externo em VM | `USAR_NAT=true bash gcloud/instalar_airflow.sh` |
| "Authorize Cloud Shell" aparece | Clique em **Authorize**: é o primeiro uso do `gcloud` na sessão |
| Projeto sem rede `default` | O script cria uma rede `default` em modo automático |
