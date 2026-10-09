# Pipeline de MLOps orquestrado com Airflow

Continuação de [`aula-mlops-experiments-registry`](https://github.com/lucas-wa/pdm-mlops-practice/tree/main/docs/aula-mlops-experiments-registry). Na aula passada o fluxo gold → treino → Experiments → Registry → Endpoint foi feito **à mão**. Hoje ele vira uma **DAG do Airflow** que prepara os dados, treina, avalia, decide por métrica e publica — e preserva o modelo em produção quando o candidato é reprovado.

> **AVISO DE CONSUMO DE CRÉDITOS.** A turma usa créditos educacionais (nada é cobrado no cartão), mas eles são finitos. Hoje dois recursos consomem **por hora, mesmo parados**: a **VM do Airflow** (`airflow-mlops`) e o **endpoint** (`rf-preco-imoveis-endpoint`). O encerramento em [`07-encerramento-custos.md`](07-encerramento-custos.md) é parte da entrega.

## Pré-requisitos

| Item | Detalhe |
|---|---|
| Projeto GCP | O mesmo da aula anterior, com faturamento ativo. Você é `Owner`. |
| Dados | A gold `aula_pdm.imoveis_gold` da aula anterior. Se não existir, o setup a recria (`gcloud/seed_gold.sh`). |
| Ambiente | Só o **Cloud Shell**. Nada a instalar na sua máquina. |
| Aula anterior | Endpoint e runtime do Colab **desligados** (o setup avisa se encontrar algo ligado). |

## A prática em 6 comandos

```bash
gcloud config set project SEU_PROJECT_ID
git clone https://github.com/Th4uan/pdm-pipeline-mlops.git
cd pdm-pipeline-mlops

bash gcloud/instalar_airflow.sh     # ~10 min; pode rodar de novo se o Cloud Shell cair
bash gcloud/30_abrir_ui.sh          # deixe aberto; Web Preview > porta 8080
bash gcloud/20_sync_dags.sh         # (em outra aba) a cada mudança nas DAGs
```

No fim: DAG `teardown_mlops` com `{"confirmar": true}` e depois `bash gcloud/99_remover_airflow.sh`.

## Ordem de leitura

| Arquivo | Conteúdo |
|---|---|
| [`00-pre-requisitos.md`](00-pre-requisitos.md) | Checklist antes da aula |
| [`01-setup-airflow-gcp.md`](01-setup-airflow-gcp.md) | Subir o Airflow, abrir a UI e enviar DAGs |
| [`02-dados-e-validacao.md`](02-dados-e-validacao.md) | `checar_gold`, `preparar_dataset` e o **TODO 1** |
| [`03-treino-e-experiments.md`](03-treino-e-experiments.md) | `treinar_e_avaliar` e o **TODO 2** |
| [`04-decisao-e-registro.md`](04-decisao-e-registro.md) | `decidir_publicacao` e o **TODO 3** |
| [`05-publicacao-endpoint.md`](05-publicacao-endpoint.md) | Publicar, testar, promover e reverter |
| [`06-falhas-controladas.md`](06-falhas-controladas.md) | Reprovação, falha de validação e idempotência |
| [`07-encerramento-custos.md`](07-encerramento-custos.md) | Teardown e varredura final |

Docentes: [`planejamento.md`](planejamento.md), [`implementacao-airflow.md`](implementacao-airflow.md) e [`roteiro-condutor.md`](roteiro-condutor.md).

## Estrutura

```
pdm-pipeline-mlops/
  README.md  planejamento.md  implementacao-airflow.md  roteiro-condutor.md
  00-pre-requisitos.md ... 07-encerramento-custos.md
  airflow/
    Dockerfile  requirements.txt  docker-compose.yaml  .env.example
    dags/
      pipeline_preco_imoveis.py   <- versão do aluno (TODOs 2 e 3)
      teardown_mlops.py
      sql/  checar_gold.sql  preparar_dataset.sql (TODO 1)  validar_dataset.sql
      comum/ config.py  modelo.py  vertex.py
    gabarito/                     <- solução completa (fora de dags/ de propósito)
    tests/                        <- DAGs de ponta a ponta com GCP simulado (bash tests/rodar_testes.sh)
  gcloud/                         <- scripts para o Cloud Shell (caminho da aula)
  terraform/                      <- o mesmo ambiente como IaC (autoestudo, a escrever)
```

## Convenções de nomes

| Recurso | Valor |
|---|---|
| Região / zona | `us-central1` / `us-central1-a` |
| Gold | `aula_pdm.imoveis_gold` |
| Tabelas por execução | `aula_pdm.treino_<sufixo>` |
| Bucket | `${PROJECT_ID}-mlops-aula` (`airflow/` e `models/rf/<sufixo>/`) |
| Experiment / Modelo / Endpoint | `preco-imoveis-rf` / `rf-preco-imoveis` / `rf-preco-imoveis-endpoint` |
| Aliases | `candidato`, `campeao` |
| VM / Service account | `airflow-mlops` / `airflow-mlops@${PROJECT_ID}.iam.gserviceaccount.com` |
| Ordem canônica das features | `[area_util, area_total, quartos, banheiros, garagens]` |
