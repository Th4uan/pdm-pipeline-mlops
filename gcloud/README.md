# Scripts para o Cloud Shell

Caminho da aula. Todos são idempotentes e leem os nomes de [`config.sh`](config.sh). Rode a partir da raiz do repositório.

| Script | O que faz |
|---|---|
| [`instalar_airflow.sh`](instalar_airflow.sh) | **Um comando:** `00_setup.sh` + `10_airflow_vm.sh` |
| [`00_setup.sh`](00_setup.sh) | Faturamento, APIs, bucket da aula, gold (via `seed_gold.sh`), quota de CPU |
| [`seed_gold.sh`](seed_gold.sh) | O da aula anterior: valida a gold e só a reconstrói se faltar ou estiver inválida |
| [`10_airflow_vm.sh`](10_airflow_vm.sh) | Service account, firewall IAP, (opcional) NAT, envio do código, VM, espera `pronto` |
| [`startup-script.sh`](startup-script.sh) | Roda **na VM**: Docker, código, sincronização das DAGs, build e `compose up` |
| [`20_sync_dags.sh`](20_sync_dags.sh) | Confere a sintaxe e envia `airflow/dags` para o bucket |
| [`30_abrir_ui.sh`](30_abrir_ui.sh) | Túnel IAP para a porta 8080 (Web Preview) |
| [`40_status.sh`](40_status.sh) | Diagnóstico: VM, fase, log, DAGs, endpoints |
| [`80_composer_demo.sh`](80_composer_demo.sh) | **Só docente:** Cloud Composer de demonstração |
| [`90_teardown_ml.sh`](90_teardown_ml.sh) | Teardown de ML sem Airflow (fallback da DAG `teardown_mlops`) |
| [`99_remover_airflow.sh`](99_remover_airflow.sh) | Remove VM, firewall, NAT e `airflow/` do bucket |

## Variáveis úteis

| Variável | Padrão | Quando mudar |
|---|---|---|
| `PROJECT_ID` | `gcloud config get-value project` | Projeto diferente do configurado |
| `VM_TYPE` | `e2-standard-4` | Quota de CPU baixa: `e2-standard-2` |
| `USAR_NAT` | `false` | Organização bloqueia IP externo |
| `REINICIAR` | `false` | Reaplicar código do Airflow numa VM existente |
| `AIRFLOW_VERSION` | `3.1.0` | Fixar outra versão no ensaio |
| `AIRFLOW_IMAGE_PRONTA` | vazio | Imagem pré-construída no Artifact Registry (pula o build) |
| `UI_PORT` | `8080` | Porta ocupada no Cloud Shell |
| `DRY_RUN` | `false` | `90_teardown_ml.sh` só mostra o que removeria |
| `GABARITO` | `false` | **Docente:** `instalar_airflow.sh` / `20_sync_dags.sh` enviam a DAG completa para demonstrar |
