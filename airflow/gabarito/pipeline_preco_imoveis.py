"""pipeline_preco_imoveis (GABARITO) — o fluxo manual da aula anterior como DAG.

definir_execucao -> checar_gold -> preparar_dataset -> validar_dataset -> treinar_e_avaliar
  -> decidir_publicacao --aprovado--> registrar_modelo -> publicar -> testar_candidato -> promover
                        \\-reprovado-> registrar_reprovacao                         \\-falha-> reverter

Regras que a DAG implementa:
  - o sufixo da execucao nomeia tabela, artefato, run do Experiments e label da versao;
  - XCom so carrega referencias (nomes, URIs, metricas), nunca dados ou modelos;
  - o campeao e' reavaliado na MESMA validacao do candidato;
  - registrar != promover: so o alias "campeao" decide quem e' oficial;
  - a versao anterior fica implantada (0% de trafego) ate o teste passar.

Parametros (Trigger DAG w/ config):
  {"n_estimators": 5, "max_depth": 1}   -> candidato fraco, reprovado
  {"min_linhas": 1000000}               -> falha na validacao dos dados
  {"forcar_falha_teste": true}          -> falha no teste do endpoint, dispara reverter
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.sdk import Param, dag, get_current_context, task

from comum import config as cfg

log = logging.getLogger(__name__)


@dag(
    dag_id="pipeline_preco_imoveis",
    schedule=None,  # disparo manual na aula; em producao: "@weekly" + sensor de dados novos
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # duas execucoes disputando o mesmo endpoint e' bug
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    params={
        "n_estimators": Param(200, type="integer", minimum=1),
        "max_depth": Param(12, type="integer", minimum=1),
        "min_linhas": Param(200, type="integer", minimum=1),
        "forcar_falha_teste": Param(False, type="boolean"),
    },
    user_defined_macros={"gold_table": cfg.GOLD_TABLE},
    tags=["mlops", "aula"],
    doc_md=__doc__,
)
def pipeline_preco_imoveis():

    @task
    def definir_execucao() -> dict:
        """Um identificador por execucao: o fio que liga dados, run, artefato e versao."""
        sufixo = pendulum.now("UTC").format("YYYYMMDDHHmmss")
        execucao = {
            "sufixo": sufixo,
            "tabela": f"{cfg.PROJETO}.{cfg.DATASET}.treino_{sufixo}",
            "gcs_dir": f"gs://{cfg.BUCKET}/models/rf/{sufixo}/",
            "run_name": f"airflow-{sufixo}",
            "dag_run_id": get_current_context()["run_id"],
        }
        log.info("execucao: %s", execucao)
        return execucao

    checar_gold = BigQueryCheckOperator(
        task_id="checar_gold",
        sql="sql/checar_gold.sql",
        use_legacy_sql=False,
        location=cfg.REGIAO,
    )

    preparar_dataset = BigQueryInsertJobOperator(
        task_id="preparar_dataset",
        configuration={
            "query": {"query": "{% include 'sql/preparar_dataset.sql' %}", "useLegacySql": False}
        },
        location=cfg.REGIAO,
    )

    validar_dataset = BigQueryCheckOperator(
        task_id="validar_dataset",
        sql="sql/validar_dataset.sql",
        use_legacy_sql=False,
        location=cfg.REGIAO,
    )

    @task
    def treinar_e_avaliar(execucao: dict) -> dict:
        """O notebook da aula anterior, reorganizado: le a tabela DESTA execucao."""
        import os
        import tempfile

        import joblib
        import numpy as np
        from google.cloud import aiplatform, bigquery, storage
        from sklearn.metrics import mean_absolute_error

        from comum import vertex
        from comum.modelo import construir_pipeline

        params = get_current_context()["params"]
        colunas = ", ".join(cfg.FEATURES + [cfg.ALVO, "conjunto"])
        df = (
            bigquery.Client(project=cfg.PROJETO)
            .query(f"SELECT {colunas} FROM `{execucao['tabela']}`")
            # API REST comum: a Storage Read API exigiria o papel bigquery.readSessionUser
            # e nao compensa para uma tabela deste tamanho
            .to_dataframe(create_bqstorage_client=False)
        )

        def separar(nome):
            parte = df[df["conjunto"] == nome]
            # .to_numpy(): o endpoint recebe array posicional, o treino tambem
            return parte[cfg.FEATURES].astype(float).to_numpy(), parte[cfg.ALVO].astype(float).to_numpy()

        X_treino, y_treino = separar("treino")
        X_val, y_val = separar("validacao")
        X_teste, y_teste = separar("teste")
        log.info("treino=%d validacao=%d teste=%d", len(y_treino), len(y_val), len(y_teste))

        pipe = construir_pipeline(n_estimators=params["n_estimators"], max_depth=params["max_depth"])
        pipe.fit(X_treino, y_treino)

        mae = float(mean_absolute_error(y_val, pipe.predict(X_val)))
        mae_baseline = float(mean_absolute_error(y_val, np.full(len(y_val), np.median(y_treino))))
        mae_teste = float(mean_absolute_error(y_teste, pipe.predict(X_teste)))

        # Campeao reavaliado na MESMA validacao: comparar MAEs de dados diferentes seria injusto.
        vertex.iniciar()
        campeao, versao_campeao = vertex.carregar_campeao()
        mae_campeao = float(mean_absolute_error(y_val, campeao.predict(X_val))) if campeao else None

        # Artefato imutavel: um diretorio por execucao, arquivo SEMPRE model.joblib.
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, "model.joblib")
            joblib.dump(pipe, local)
            destino = execucao["gcs_dir"].removeprefix(f"gs://{cfg.BUCKET}/") + "model.joblib"
            storage.Client(project=cfg.PROJETO).bucket(cfg.BUCKET).blob(destino).upload_from_filename(local)

        # Run no Experiments com o caminho ate os dados que o geraram.
        vertex.iniciar(experimento=True)
        try:
            aiplatform.start_run(execucao["run_name"])
        except Exception:  # retry da tarefa: o run ja existe
            aiplatform.start_run(execucao["run_name"], resume=True)
        aiplatform.log_params({
            "modelo": "RandomForestRegressor",
            "n_estimators": params["n_estimators"],
            "max_depth": params["max_depth"],
            "features": ",".join(cfg.FEATURES),
            "tabela": execucao["tabela"],
            "artefato": execucao["gcs_dir"],
            "dag_run_id": execucao["dag_run_id"],
            "campeao_comparado": str(versao_campeao or "nenhum"),
        })
        metricas = {"mae": mae, "mae_baseline": mae_baseline, "mae_teste": mae_teste}
        if mae_campeao is not None:
            metricas["mae_campeao"] = mae_campeao
        aiplatform.log_metrics(metricas)
        aiplatform.end_run()

        resultado = {**metricas, "mae_campeao": mae_campeao, "gcs_dir": execucao["gcs_dir"]}
        log.info("resultado: %s", resultado)
        return resultado

    @task.branch
    def decidir_publicacao(resultado: dict) -> str:
        """A regra escrita em codigo — antes ela estava na cabeca de quem olhava o notebook."""
        bate_baseline = resultado["mae"] < resultado["mae_baseline"]
        bate_campeao = resultado["mae_campeao"] is None or resultado["mae"] < resultado["mae_campeao"]
        log.info("bate_baseline=%s bate_campeao=%s", bate_baseline, bate_campeao)
        return "registrar_modelo" if bate_baseline and bate_campeao else "registrar_reprovacao"

    @task(retries=0)
    def registrar_modelo(execucao: dict, resultado: dict) -> dict:
        """Registrar e' gratis e NAO publica: a versao entra com alias 'candidato'."""
        from google.cloud import aiplatform

        from comum import vertex

        vertex.iniciar()
        pai = vertex.modelo_pai()
        modelo = aiplatform.Model.upload(
            display_name=cfg.MODELO,
            parent_model=pai.resource_name if pai else None,  # versao do MESMO modelo
            is_default_version=pai is None,
            artifact_uri=resultado["gcs_dir"],  # o DIRETORIO, nunca o arquivo
            serving_container_image_uri=cfg.SERVING_CONTAINER,
            labels={"execucao": execucao["sufixo"], "origem": "airflow"},
            version_description=f"{execucao['run_name']} | MAE validacao R$ {resultado['mae']:,.0f}",
            sync=True,
        )
        vertex.mover_alias(cfg.ALIAS_CANDIDATO, modelo.version_id)
        vertex.iniciar(experimento=True)
        aiplatform.start_run(execucao["run_name"], resume=True)
        aiplatform.log_params({"decisao": "aprovado", "versao_registry": str(modelo.version_id)})
        aiplatform.end_run()
        return {"model_name": modelo.versioned_resource_name, "version_id": str(modelo.version_id)}

    @task
    def registrar_reprovacao(execucao: dict, resultado: dict) -> None:
        """Reprovado nao some: fica no Experiments como evidencia da decisao."""
        from google.cloud import aiplatform

        from comum import vertex

        vertex.iniciar(experimento=True)
        aiplatform.start_run(execucao["run_name"], resume=True)
        aiplatform.log_params({"decisao": "reprovado"})
        aiplatform.end_run()
        log.info("Candidato reprovado (%s). O endpoint segue com o campeao atual.", resultado)

    @task(retries=0, execution_timeout=timedelta(minutes=40))
    def publicar(registro: dict) -> dict:
        """Blue/green: a versao nova recebe 100% e a anterior fica implantada a 0%."""
        from google.cloud import aiplatform

        from comum import vertex

        vertex.iniciar()
        existentes = aiplatform.Endpoint.list(filter=f'display_name="{cfg.ENDPOINT}"')
        endpoint = existentes[0] if existentes else aiplatform.Endpoint.create(display_name=cfg.ENDPOINT)
        anteriores = [m.id for m in endpoint.list_models()]

        modelo = aiplatform.Model(registro["model_name"])
        modelo.deploy(  # 10 a 20 minutos
            endpoint=endpoint,
            deployed_model_display_name=f"{cfg.MODELO}-v{registro['version_id']}",
            machine_type=cfg.MAQUINA_ENDPOINT,
            min_replica_count=1,
            max_replica_count=1,
            traffic_percentage=100,
            sync=True,
        )
        endpoint = aiplatform.Endpoint(endpoint.resource_name)
        novo = [m.id for m in endpoint.list_models() if m.id not in anteriores][0]
        return {"endpoint": endpoint.resource_name, "novo_id": novo, "anteriores": anteriores}

    @task(retries=1)
    def testar_candidato(publicacao: dict, registro: dict) -> None:
        """Teste de servico: responde, com a versao certa e um valor plausivel."""
        from google.cloud import aiplatform

        from comum import vertex

        if get_current_context()["params"]["forcar_falha_teste"]:
            raise RuntimeError("Falha forcada pelo parametro forcar_falha_teste (demonstracao de reversao).")

        vertex.iniciar()
        resposta = aiplatform.Endpoint(publicacao["endpoint"]).predict(instances=cfg.PAYLOAD_REFERENCIA)
        preco = float(resposta.predictions[0])
        log.info("predicao=%.2f versao=%s", preco, resposta.model_version_id)
        if str(resposta.model_version_id) != registro["version_id"]:
            raise RuntimeError(f"Respondeu a versao {resposta.model_version_id}, esperado {registro['version_id']}.")
        minimo, maximo = cfg.FAIXA_PLAUSIVEL
        if not minimo <= preco <= maximo:
            raise RuntimeError(f"Predicao fora da faixa plausivel: R$ {preco:,.2f}")

    @task
    def promover(publicacao: dict, registro: dict) -> None:
        """Teste ok: alias campeao muda de dono e a versao anterior sai do endpoint."""
        from google.cloud import aiplatform

        from comum import vertex

        vertex.iniciar()
        endpoint = aiplatform.Endpoint(publicacao["endpoint"])
        for antigo in publicacao["anteriores"]:
            endpoint.undeploy(deployed_model_id=antigo, sync=True)
        vertex.mover_alias(cfg.ALIAS_CAMPEAO, registro["version_id"])
        log.info("Versao %s e' a nova campea.", registro["version_id"])

    @task(trigger_rule="one_failed", retries=0)
    def reverter() -> None:
        """Teste falhou: trafego volta para a anterior, sem novo deploy de 10-20 min."""
        from google.cloud import aiplatform

        from comum import vertex

        publicacao = get_current_context()["ti"].xcom_pull(task_ids="publicar")
        if not publicacao:
            log.info("Nada foi publicado; nada a reverter.")
            return
        vertex.iniciar()
        endpoint = aiplatform.Endpoint(publicacao["endpoint"])
        anteriores = publicacao["anteriores"]
        if anteriores:
            endpoint.undeploy(
                deployed_model_id=publicacao["novo_id"],
                traffic_split={anteriores[0]: 100},
                sync=True,
            )
            log.info("Trafego devolvido ao deployment %s.", anteriores[0])
        else:
            endpoint.undeploy(deployed_model_id=publicacao["novo_id"], sync=True)
            log.info("Nao havia versao anterior: candidato retirado, endpoint vazio.")

    execucao = definir_execucao()
    execucao >> checar_gold >> preparar_dataset >> validar_dataset

    resultado = treinar_e_avaliar(execucao)
    validar_dataset >> resultado

    decisao = decidir_publicacao(resultado)
    registro = registrar_modelo(execucao, resultado)
    reprovacao = registrar_reprovacao(execucao, resultado)
    decisao >> [registro, reprovacao]

    publicacao = publicar(registro)
    teste = testar_candidato(publicacao, registro)
    teste >> promover(publicacao, registro)
    teste >> reverter()


pipeline_preco_imoveis()
