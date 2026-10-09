# 01 — Subir o Airflow na GCP pelo Cloud Shell

Tudo roda no **Cloud Shell**. O trabalho pesado acontece **dentro da VM**: se o Cloud Shell cair, rode o mesmo comando de novo e ele volta a acompanhar de onde parou.

## 1. Instalar

```bash
gcloud config set project SEU_PROJECT_ID
git clone https://github.com/Th4uan/pdm-pipeline-mlops.git
cd pdm-pipeline-mlops
bash gcloud/instalar_airflow.sh
```

O que acontece:

| Script | Faz |
|---|---|
| [`00_setup.sh`](gcloud/00_setup.sh) | Confere faturamento; habilita `compute`, `iap`, `aiplatform`, `bigquery`, `storage`, `logging`; cria o bucket `${PROJECT_ID}-mlops-aula`; garante a gold; mostra a quota de CPU e avisa sobre endpoints ligados |
| [`10_airflow_vm.sh`](gcloud/10_airflow_vm.sh) | Cria a service account `airflow-mlops` com papéis mínimos (escrita só no dataset `aula_pdm` e no bucket da aula); firewall liberando portas 22 e 8080 **só para o IAP**; envia o código do Airflow ao bucket; cria a VM; espera o status `pronto` |
| [`startup-script.sh`](gcloud/startup-script.sh) | Roda **na VM**: instala Docker, baixa o código, liga a sincronização das DAGs (30 s), constrói a imagem e sobe o Docker Compose |

Enquanto espera, o terminal mostra as fases: `instalando-docker` → `baixando-codigo` → `sincronizando-dags` → `construindo-imagem` → `iniciando-airflow` → `pronto`.

## 2. Abrir a interface

Na **aba 1** do Cloud Shell (deixe rodando):

```bash
bash gcloud/30_abrir_ui.sh
```

Depois: **Web Preview** (ícone no topo do Cloud Shell) → **Preview on port 8080**. A UI do Airflow abre sem login — a porta só é alcançável pelo túnel IAP, que exige ser principal do projeto.

Use uma **aba 2** para os demais comandos.

**Checkpoint:** as DAGs `pipeline_preco_imoveis` e `teardown_mlops` aparecem, sem *DAG Import Errors*.

## 3. Enviar DAGs

```
airflow/dags  --20_sync_dags.sh-->  gs://${PROJECT_ID}-mlops-aula/airflow/dags  --VM, a cada 30 s-->  Airflow
```

```bash
bash gcloud/20_sync_dags.sh
```

- Edite pelo **Cloud Shell Editor** (botão *Open Editor*), em `airflow/dags/`.
- O script confere a sintaxe antes de enviar.
- A DAG nova ou alterada aparece na UI em até ~1 minuto.
- Apagar um arquivo local e sincronizar remove a DAG do Airflow.
- Ficou para trás? `cp airflow/gabarito/pipeline_preco_imoveis.py airflow/dags/ && cp airflow/gabarito/sql/preparar_dataset.sql airflow/dags/sql/ && bash gcloud/20_sync_dags.sh`

## 4. Diagnóstico

```bash
bash gcloud/40_status.sh
```

Mostra o estado da VM, a fase da instalação, as últimas linhas do log da VM, as DAGs no bucket e os endpoints/modelos ligados.

| Sintoma | Causa provável |
|---|---|
| Status `erro:construindo-imagem` | Falha no `pip install`; veja o log no `40_status.sh` e rode `REINICIAR=true bash gcloud/10_airflow_vm.sh` |
| Web Preview não abre | Túnel não está rodando na aba 1, ou a porta é outra (`UI_PORT=8081`) |
| DAG não aparece | Esqueceu o `20_sync_dags.sh`, ou espere 1 min; erro de import aparece no topo da UI |
| Tarefa falha com `403` | Papel faltando na service account: rode de novo `bash gcloud/10_airflow_vm.sh` |
