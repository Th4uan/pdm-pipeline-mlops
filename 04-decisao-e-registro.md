# 04 — Decisão por métrica e registro

## TODO 3 — a regra de aprovação

`decidir_publicacao` é um `@task.branch`: devolve o `task_id` do caminho a seguir; o outro fica *skipped*.

Aprovar **somente** se:

- `mae < mae_baseline` **e**
- não existe campeão (`mae_campeao is None`) **ou** `mae < mae_campeao`.

```python
@task.branch
def decidir_publicacao(resultado: dict) -> str:
    bate_baseline = resultado["mae"] < resultado["mae_baseline"]
    bate_campeao = resultado["mae_campeao"] is None or resultado["mae"] < resultado["mae_campeao"]
    return "registrar_modelo" if bate_baseline and bate_campeao else "registrar_reprovacao"
```

Sem o TODO 3 a DAG reprova tudo.

Empate é reprovação: rodar de novo com os mesmos parâmetros produz o mesmo modelo e não troca o campeão.

## Registrar ≠ promover

| Tarefa | Faz |
|---|---|
| `registrar_modelo` | `Model.upload` com `parent_model` (versão do mesmo `rf-preco-imoveis`), `artifact_uri` = diretório da execução, label `execucao=<sufixo>`, alias **`candidato`** |
| `registrar_reprovacao` | Grava `decisao=reprovado` no run do Experiments. Nada é publicado |

O alias **`campeao`** só é movido depois do teste do endpoint, em `promover` ([05](05-publicacao-endpoint.md)).

## Conferir

**Vertex AI → Model Registry → `rf-preco-imoveis`** → aba de versões: a versão nova com alias `candidato` e label `execucao`.
