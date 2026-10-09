# 06 — Falhas controladas

Dispare pela UI com **Trigger DAG w/ config**:

| Config | O que acontece | O que observar |
|---|---|---|
| `{"n_estimators": 5, "max_depth": 1}` | Candidato perde do campeão | `registrar_reprovacao` verde; `publicar` e seguintes *skipped*; endpoint responde com o campeão |
| `{"min_linhas": 1000000}` | `validar_dataset` falha | Tentativas (`up_for_retry`) e parada; nada a jusante roda |
| `{"max_depth": 20, "forcar_falha_teste": true}` | Candidato aprovado é publicado, mas o teste falha | `reverter` devolve o tráfego; `promover` não roda |
| **Clear** numa tarefa verde | Ela roda de novo | Nada duplicado: `CREATE OR REPLACE`, endpoint reaproveitado |

A terceira linha só demonstra a reversão se o candidato **vencer** o campeão (empate é reprovação). Confirme no ensaio um `max_depth` que vence na gold da turma. Ela ocupa o endpoint por mais 10–20 min.

Depois de cada uma, rode a predição de [05](05-publicacao-endpoint.md) e confira qual versão segue implantada.
