"""teardown_mlops — encerra os recursos de ML da aula, do mais caro ao mais barato.

1. undeploy de todos os modelos do endpoint e delete do endpoint (consome por hora, 24/7);
2. versoes e modelo rf-preco-imoveis no Model Registry;
3. artefatos em gs://<bucket>/models/rf/;
4. tabelas aula_pdm.treino_* criadas pelo pipeline.

PROTEGIDOS: a gold (aula_pdm.imoveis_gold), o bucket <projeto>-aula-pdm e o experimento.
Disparo: Trigger DAG w/ config {"confirmar": true}. Depois, no Cloud Shell:
  bash gcloud/99_remover_airflow.sh   (a DAG nao consegue apagar a VM em que roda)
"""

from __future__ import annotations

import logging

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from comum import config as cfg

log = logging.getLogger(__name__)


@dag(
    dag_id="teardown_mlops",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    params={"confirmar": Param(False, type="boolean", description="Marque para confirmar a remocao")},
    tags=["mlops", "aula", "custos"],
    doc_md=__doc__,
)
def teardown_mlops():

    @task
    def conferir_confirmacao() -> None:
        if not get_current_context()["params"]["confirmar"]:
            raise ValueError('Nada removido. Dispare com {"confirmar": true}.')

    @task
    def remover_endpoint() -> None:
        from google.cloud import aiplatform

        from comum import vertex

        vertex.iniciar()
        for endpoint in aiplatform.Endpoint.list(filter=f'display_name="{cfg.ENDPOINT}"'):
            endpoint.undeploy_all(sync=True)  # nao se apaga endpoint (nem modelo) implantado
            endpoint.delete(sync=True)
            log.info("Endpoint removido: %s", endpoint.resource_name)

    @task
    def remover_modelo() -> None:
        from google.cloud import aiplatform

        from comum import vertex

        vertex.iniciar()
        for modelo in aiplatform.Model.list(filter=f'display_name="{cfg.MODELO}"'):
            registro = modelo.versioning_registry
            for versao in registro.list_versions():
                if "default" not in (versao.version_aliases or []):
                    registro.delete_version(version=versao.version_id)
            modelo.delete(sync=True)  # a versao default sai com o modelo
            log.info("Modelo removido: %s", modelo.resource_name)

    @task
    def remover_artefatos() -> None:
        from google.cloud import storage

        bucket = storage.Client(project=cfg.PROJETO).bucket(cfg.BUCKET)
        blobs = list(bucket.list_blobs(prefix="models/rf/"))
        for blob in blobs:
            blob.delete()
        log.info("%d objetos removidos de gs://%s/models/rf/", len(blobs), cfg.BUCKET)

    @task
    def remover_tabelas_treino() -> None:
        from google.cloud import bigquery

        cliente = bigquery.Client(project=cfg.PROJETO)
        for tabela in cliente.list_tables(f"{cfg.PROJETO}.{cfg.DATASET}"):
            if tabela.table_id.startswith("treino_"):  # nunca a gold
                cliente.delete_table(tabela.reference)
                log.info("Tabela removida: %s", tabela.table_id)

    conferir_confirmacao() >> remover_endpoint() >> remover_modelo() >> [remover_artefatos(), remover_tabelas_treino()]


teardown_mlops()
