# 03 — Treino e Experiments dentro da DAG

`treinar_e_avaliar` é o notebook da aula anterior reorganizado:

1. lê **a tabela desta execução** (`treino_<sufixo>`), não a gold;
2. treina `construir_pipeline()` — a mesma de [`comum/modelo.py`](airflow/dags/comum/modelo.py), com as mesmas restrições de serving;
3. calcula `mae`, `mae_baseline` (mediana do treino) e `mae_teste`;
4. **reavalia o campeão atual na mesma validação** (`mae_campeao`), se existir;
5. salva o artefato em `gs://${PROJECT_ID}-mlops-aula/models/rf/<sufixo>/model.joblib` — um diretório por execução, nada sobrescrito;
6. registra o run `airflow-<sufixo>` no experimento `preco-imoveis-rf`;
7. devolve pelo XCom **só referências e números**.

O treino usa arrays posicionais (`.to_numpy()`), igual ao que o endpoint recebe.

## TODO 2 — o run aponta para os dados

Em `airflow/dags/pipeline_preco_imoveis.py`, complete o `log_params` e o `log_metrics`:

```python
aiplatform.log_params({
    "modelo": "RandomForestRegressor",
    "n_estimators": params["n_estimators"],
    "max_depth": params["max_depth"],
    "features": ",".join(cfg.FEATURES),
    "tabela": execucao["tabela"],
    "artefato": execucao["gcs_dir"],
    "dag_run_id": execucao["dag_run_id"],
})
aiplatform.log_metrics(metricas)
```

## Conferir

Console → **Vertex AI → Experiments → `preco-imoveis-rf`**: os runs `airflow-…` aparecem ao lado de `run-a-…` e `run-b-…` da aula passada, agora com a coluna `tabela`.
