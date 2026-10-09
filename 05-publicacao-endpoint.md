# 05 — Publicação no endpoint

> **Consome créditos por hora.** O endpoint `rf-preco-imoveis-endpoint` usa uma `n1-standard-2` ligada 24/7 até o teardown.

| Tarefa | Faz |
|---|---|
| `publicar` | Cria o endpoint se não existir; implanta a versão com **100% do tráfego**, mantendo a anterior implantada a 0%. Leva **10 a 20 min**. Sem retries automáticos (evita implantações duplicadas) |
| `testar_candidato` | Envia `[[120.0, 150.0, 3, 2, 1]]`; confere que respondeu a versão nova (`model_version_id`) e um preço entre R$ 10 mil e R$ 50 milhões |
| `promover` | Teste ok: retira a versão anterior do endpoint e move o alias **`campeao`** |
| `reverter` | Teste falhou (`trigger_rule="one_failed"`): devolve 100% à anterior e retira o candidato, sem novo deploy |

A primeira execução aprovada cria o campeão. A partir daí cada candidato precisa vencê-lo.

## Conferir a versão que responde

No Cloud Shell:

```bash
ENDPOINT_ID=$(gcloud ai endpoints list --region=us-central1 \
  --filter=displayName=rf-preco-imoveis-endpoint --format='value(name.basename())')
echo '{"instances": [[120.0, 150.0, 3, 2, 1]]}' > /tmp/req.json
gcloud ai endpoints predict "$ENDPOINT_ID" --region=us-central1 --json-request=/tmp/req.json
```

A resposta traz `predictions` e o `deployedModelId`. Confira em **Vertex AI → Online prediction → `rf-preco-imoveis-endpoint`** qual versão está implantada: deve ser a que tem o alias `campeao` no Registry.

## Contingência

Deploy passou de 25 min: siga a aula com o endpoint de referência do docente; a sua execução conclui sozinha antes do teardown. Erro `System error. Please try this operation again.`: falha transitória do Vertex; **Clear** em `publicar`.
