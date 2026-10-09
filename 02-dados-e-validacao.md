# 02 — Dados: checar, preparar e validar

| Tarefa | Operador | Arquivo |
|---|---|---|
| `definir_execucao` | `@task` | gera o `sufixo` (UTC, `YYYYMMDDHHmmss`) e os nomes da execução |
| `checar_gold` | `BigQueryCheckOperator` | [`airflow/dags/sql/checar_gold.sql`](airflow/dags/sql/checar_gold.sql) |
| `preparar_dataset` | `BigQueryInsertJobOperator` | [`airflow/dags/sql/preparar_dataset.sql`](airflow/dags/sql/preparar_dataset.sql) |
| `validar_dataset` | `BigQueryCheckOperator` | [`airflow/dags/sql/validar_dataset.sql`](airflow/dags/sql/validar_dataset.sql) |

`BigQueryCheckOperator` executa a consulta e **falha se qualquer coluna da primeira linha for falsa** — é assim que a DAG para antes de treinar com dado ruim.

`preparar_dataset` cria `aula_pdm.treino_<sufixo>`: uma linha por imóvel, só as colunas do contrato e a coluna `conjunto`. Essa tabela é o **snapshot** que torna o treino reproduzível. A gold nunca é alterada.

## TODO 1 — split por imóvel

Em `airflow/dags/sql/preparar_dataset.sql`, troque `'treino' AS conjunto` por um `CASE` que separe 70 / 15 / 15 usando:

```sql
MOD(ABS(FARM_FINGERPRINT(CAST(id AS STRING))), 100)   -- 0 a 99, sempre igual para o mesmo id
```

Por quê: na aula passada o split dependia de `ORDER BY id` + `random_state=42`. Com o hash, **o imóvel decide o próprio conjunto**, independente da ordem das linhas e de quem executa.

Enquanto o TODO 1 não estiver feito, `validar_dataset` falha (`tem_validacao` = false).

## Conferir

```bash
bash gcloud/20_sync_dags.sh
```

UI → `pipeline_preco_imoveis` → **Trigger**. Acompanhe até `validar_dataset` e confira no BigQuery:

```sql
SELECT conjunto, COUNT(*) AS imoveis
FROM `SEU_PROJECT_ID.aula_pdm.treino_<sufixo>`
GROUP BY conjunto;
```

Esperado: proporção próxima de 70 / 15 / 15.
