# Roteiro do condutor — Pipeline de MLOps com Airflow (~180 min)

Slides: deck da aula (34 slides, com notas do apresentador). Planejamento: [`planejamento.md`](planejamento.md). Ambiente: [`implementacao-airflow.md`](implementacao-airflow.md).

---

## Antes de entrar na sala

- [ ] Ambiente de demonstração do docente com a DAG completa: `GABARITO=true bash gcloud/instalar_airflow.sh` (ou, numa VM já criada, `GABARITO=true bash gcloud/20_sync_dags.sh`).
- [ ] Testes offline passando: `cd airflow && bash tests/rodar_testes.sh` (simula a GCP; não cria nada na nuvem).
- [ ] Ensaio completo num projeto limpo: `instalar_airflow.sh` → TODOs pelo gabarito → execução aprovada → reprovada → reversão → `teardown_mlops` → `99_remover_airflow.sh`. Anotar o tempo de cada fase.
- [ ] `max_depth` que **vence** o campeão padrão na gold da turma (para a demonstração de reversão).
- [ ] VM e endpoint **de referência** do docente ligados (contingência).
- [ ] Composer de demonstração criado na véspera, se for mostrado (`80_composer_demo.sh`).
- [ ] Turma recebeu, na véspera, os comandos do slide "Provisionar" para rodar antes da aula.
- [ ] Link final do repositório confirmado nos slides "Repositório" e "Provisionar".
- [ ] Abas abertas: UI do Airflow (referência), BigQuery, Vertex AI Experiments, Model Registry, Endpoints, Compute Engine.

> **Regra de ouro do tempo.** O deploy leva 10–20 min e não acelera. A execução completa é disparada no minuto ~115, e a publicação é explicada enquanto ela roda.

---

## 000–010 · Abertura

| Slide | Fala-chave |
|---|---|
| Capa | "Na sexta passada fizemos tudo à mão. Hoje o mesmo fluxo vira um pipeline." |
| Onde paramos | Mostrar Experiments e Registry da aula passada. "Quem lembra qual dado treinou a v2?" |
| O que era manual vira tarefa | Uma linha por vez. "Vocês foram o orquestrador." |
| Objetivo de hoje | Avisar: VM e endpoint custam por hora; o encerramento não é cortado |

**Se a turma não instalou antes:** mostrar o slide "Provisionar" agora, todos disparam `instalar_airflow.sh` e seguimos com os conceitos enquanto a VM sobe.

## 010–050 · Conceitos (o que é e por que usar cada coisa)

| Tempo | Slide | Fala-chave |
|---|---|---|
| 010–014 | MLOps | Reproduzível, auditável, reversível — e agora automático |
| 014–018 | Pipeline e orquestração | O maestro não toca: coordena, espera, decide |
| 018–022 | O que é o Airflow | Workflows como código Python; UI; providers; Composer |
| 022–027 | Por que Airflow | Comparar com notebook, cron, Vertex AI Pipelines |
| 027–032 | O que é uma DAG | Directed, Acyclic, Graph; DAG Run e Task Instance |
| 032–036 | Tarefas e operadores | Operador pronto × `@task` × `@task.branch` |
| 036–040 | Por dentro do Airflow | DAG processor, scheduler, executor, banco, UI |
| 040–043 | XCom e parâmetros | "XCom é bilhete, não caminhão" |
| 043–046 | Falhar bem | Retries, idempotência, trigger rules |
| 046–049 | Ferramentas e por quê | Demorar em Compute Engine, IAP, service account |
| 049–050 | VM × Composer | Mesmo arquivo de DAG; muda quem opera |

## 050–065 · Arquitetura

| Slide | Fala-chave |
|---|---|
| Arquitetura do ambiente | O pesado roda na VM; o Cloud Shell só dispara e consulta |
| Como a DAG chega no Airflow | Bucket como fonte da verdade, igual ao Composer |
| A DAG | Desenhar no quadro; o losango é a decisão que era humana |
| Regras | Voltar a este slide quando cada regra aparecer no código |

## 065–080 · Provisionar e abrir a UI

| Slide | Ação |
|---|---|
| Repositório / Provisionar / O que o script faz | Quem já instalou confere com `40_status.sh` |
| Abrir a interface | Aba 1: `30_abrir_ui.sh`; Web Preview 8080 |
| Os dados | A gold da aula passada; o seed só reconstrói se faltar |

**Checkpoint:** todos veem `pipeline_preco_imoveis` e `teardown_mlops` na UI.

## 080–120 · TODOs

| Tempo | Slide | Ação | Checkpoint |
|---|---|---|---|
| 080–095 | TODO 1 | Completar o `CASE`; sync; Trigger; acompanhar até `validar_dataset` | `treino_<sufixo>` ~70/15/15 |
| 095–110 | TODO 2 | Completar `log_params`/`log_metrics` | Run `airflow-…` com `tabela` |
| 110–115 | TODO 3 | Escrever a regra do branch | — |
| 115–120 | Enviar, disparar, acompanhar | **Sync + Trigger da execução completa** | Deploy em andamento |

Aluno travado: copiar o gabarito (comando em [`01-setup-airflow-gcp.md`](01-setup-airflow-gcp.md)) e seguir.

## 120–140 · Publicação

Explicar `publicar` → `testar_candidato` → `promover` / `reverter` enquanto o deploy roda. Ao terminar: predição com a versão campeã ([`05`](05-publicacao-endpoint.md)).

## 140–155 · Falhas controladas

1. `{"n_estimators": 5, "max_depth": 1}` — todos. Reprovado; endpoint igual.
2. `{"min_linhas": 1000000}` — todos. Retries e parada.
3. `{"max_depth": <vence>, "forcar_falha_teste": true}` — **só o docente**, se houver tempo.
4. **Clear** numa tarefa verde — idempotência.

## 155–170 · Encerramento (nunca cortar)

`teardown_mlops` com `{"confirmar": true}` → `99_remover_airflow.sh` → varredura de [`07`](07-encerramento-custos.md). Reforçar em voz alta: **não apagar** `${PROJECT_ID}-aula-pdm` nem a gold.

## 170–180 · Fechamento

Entregas (slide "Entregas"), próximos passos, Q&A.

---

## Se o tempo apertar, cortar nesta ordem

1. Reversão forçada (falha 3).
2. Falha de validação (falha 2) e Clear.
3. Slide "Por dentro do Airflow" e "VM × Composer" (resumir em 1 min).
4. TODO 2 — entregar pronto pelo gabarito.

**Nunca cortar:** encerramento.
