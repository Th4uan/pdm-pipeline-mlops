# 07 — Encerramento e custos

Ao vivo, todos juntos, na ordem.

## 1. Recursos de ML — DAG `teardown_mlops`

UI → `teardown_mlops` → **Trigger DAG w/ config** → `{"confirmar": true}`.

| Tarefa | Remove |
|---|---|
| `remover_endpoint` | Undeploy de todas as versões e delete do `rf-preco-imoveis-endpoint` (maior custo) |
| `remover_modelo` | Versões e modelo `rf-preco-imoveis` |
| `remover_artefatos` | `gs://${PROJECT_ID}-mlops-aula/models/rf/` |
| `remover_tabelas_treino` | `aula_pdm.treino_*` (nunca a gold) |

Sem Airflow, ou a DAG falhou: `bash gcloud/90_teardown_ml.sh` faz o mesmo (`DRY_RUN=true` só mostra).

## 2. Ambiente Airflow — Cloud Shell

A DAG não consegue apagar a VM em que roda:

```bash
bash gcloud/99_remover_airflow.sh
```

Remove a VM, o firewall do IAP, o Cloud NAT (se criado) e `gs://${PROJECT_ID}-mlops-aula/airflow/`. Avisa antes se o endpoint ainda existir.

> **NÃO APAGAR:** o bucket `${PROJECT_ID}-aula-pdm` e a gold `aula_pdm.imoveis_gold`. Os scripts não tocam neles. O experimento `preco-imoveis-rf` pode ficar: é a evidência da entrega e não gera custo relevante.

## 3. Varredura final (região `us-central1`)

| Onde | Esperado |
|---|---|
| Compute Engine → VM instances | Sem `airflow-mlops` |
| Vertex AI → Online prediction → Endpoints | Vazio |
| Vertex AI → Model Registry | Sem `rf-preco-imoveis` |
| Cloud Storage → `${PROJECT_ID}-mlops-aula` | Sem `models/rf/` e sem `airflow/` |
| BigQuery → `aula_pdm` | Só a gold (e a bronze `anuncios`, se o seed rodou) |
