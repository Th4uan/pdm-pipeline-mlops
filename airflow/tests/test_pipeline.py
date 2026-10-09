"""Fluxo de MLOps de ponta a ponta, sem GCP.

As DAGs rodam de verdade (dag.test, Airflow 3) contra o GCP simulado de fake_gcp.py:
o SQL da DAG roda no DuckDB, o modelo e' treinado, salvo, registrado, "implantado" e o
endpoint faz predicao carregando o model.joblib salvo.
"""

import fake_gcp as f
from conftest import carregar_dag, preparar, rodar

PROD = "rf-preco-imoveis-endpoint"
MODELO = "rf-preco-imoveis"


def versao_em_producao():
    """(versao, trafego) de quem responde no endpoint."""
    deps = f.endpoints_resumo()[PROD]
    return max(deps.values(), key=lambda v: v[1])


def aliases():
    return f.modelos_resumo()[MODELO]


def run(prefixo=""):
    return f.ESTADO.runs[sorted(f.ESTADO.runs)[-1]]


def test_gabarito_fluxo_completo_ate_producao(gcp):
    dag = carregar_dag("pipeline_preco_imoveis", gabarito=True)

    # 1. Primeira execucao: nao ha campeao, compara so com o baseline e vai para producao.
    estados = rodar(dag, n_estimators=20, max_depth=4)
    assert estados["promover"] == "success", estados
    assert estados["registrar_reprovacao"] == "skipped"
    assert estados["reverter"] == "skipped"
    assert aliases() == {"1": ["campeao", "candidato", "default"]}
    assert versao_em_producao() == ("1", 100)
    assert len(f.endpoints_resumo()[PROD]) == 1
    r1 = run()
    assert r1["params"]["decisao"] == "aprovado"
    assert r1["params"]["tabela"].startswith("proj-teste.aula_pdm.treino_")
    assert r1["metrics"]["mae"] < r1["metrics"]["mae_baseline"]

    # split por hash: ~70/15/15 e nenhum imovel em dois conjuntos
    tabela = [t for t in f.tabelas() if t.startswith("treino_")][0]
    contagem = f.executar_sql(f"SELECT conjunto, COUNT(*) n FROM aula_pdm.{tabela} GROUP BY conjunto")
    prop = dict(zip(contagem.conjunto, contagem.n / contagem.n.sum()))
    assert abs(prop["treino"] - 0.70) < 0.05 and abs(prop["validacao"] - 0.15) < 0.04, prop

    # artefato imutavel por execucao, nome model.joblib
    assert any(o.endswith("/model.joblib") and "/models/rf/2" in o for o in f.objetos())

    # predicao real do endpoint com o payload canonico
    from google.cloud import aiplatform
    ep = aiplatform.Endpoint.list(filter=f'display_name="{PROD}"')[0]
    resp = ep.predict(instances=[[120.0, 150.0, 3, 2, 1]])
    assert 10_000 < resp.predictions[0] < 50_000_000 and resp.model_version_id == "1"

    # 2. Candidato fraco: reprovado, producao intacta.
    estados = rodar(dag, n_estimators=5, max_depth=1)
    assert estados["registrar_reprovacao"] == "success", estados
    assert estados["registrar_modelo"] == "skipped"
    assert estados["publicar"] == "skipped" and estados["promover"] == "skipped"
    assert run()["params"]["decisao"] == "reprovado"
    assert run()["metrics"]["mae"] >= run()["metrics"]["mae_campeao"]
    assert versao_em_producao() == ("1", 100)
    assert list(aliases()) == ["1"]  # nada registrado

    # 3. Dados insuficientes: para na validacao, nada a jusante roda.
    estados = rodar(dag, min_linhas=1_000_000)
    assert estados["validar_dataset"] == "failed", estados
    assert estados["treinar_e_avaliar"] == "upstream_failed"
    assert versao_em_producao() == ("1", 100)

    # 4. Candidato melhor, mas o teste do endpoint falha: reverte sem novo deploy.
    estados = rodar(dag, forcar_falha_teste=True)
    assert estados["publicar"] == "success", estados
    assert estados["testar_candidato"] == "failed"
    assert estados["reverter"] == "success"
    assert estados["promover"] == "upstream_failed"
    assert versao_em_producao() == ("1", 100)
    assert len(f.endpoints_resumo()[PROD]) == 1  # candidato retirado
    assert "campeao" in aliases()["1"] and "candidato" in aliases()["2"]

    # 5. Candidato melhor e teste ok: novo campeao, versao anterior sai do endpoint.
    estados = rodar(dag)
    assert estados["promover"] == "success", estados
    assert versao_em_producao() == ("3", 100)
    assert len(f.endpoints_resumo()[PROD]) == 1
    assert "campeao" in aliases()["3"] and "campeao" not in aliases()["1"]
    assert run()["params"]["campeao_comparado"] == "1"

    # 6. Teardown: sem confirmacao nao remove; com confirmacao remove tudo menos a gold.
    teardown = carregar_dag("teardown_mlops", gabarito=True)
    estados = rodar(teardown)
    assert estados["conferir_confirmacao"] == "failed"
    assert PROD in f.endpoints_resumo()

    estados = rodar(teardown, confirmar=True)
    assert all(s == "success" for s in estados.values()), estados
    assert f.endpoints_resumo() == {} and f.modelos_resumo() == {}
    assert f.objetos("proj-teste-mlops-aula/models/") == []
    assert f.tabelas() == ["imoveis_gold"]


def test_versao_do_aluno_sem_todos(gcp):
    """Sem os TODOs a DAG nao quebra a producao: para no TODO 1 e, depois dele, reprova tudo (TODO 3)."""
    dag = carregar_dag("pipeline_preco_imoveis", gabarito=False)

    # TODO 1 pendente: tudo cai em 'treino', a validacao barra.
    estados = rodar(dag)
    assert estados["validar_dataset"] == "failed", estados

    # Com o TODO 1 resolvido (SQL do gabarito), TODO 2 e 3 ainda pendentes.
    import shutil
    from conftest import AIRFLOW_DIR, TMP
    dag_sql = TMP / "dags-aluno-todo1" / "sql"
    shutil.copytree(AIRFLOW_DIR / "dags", TMP / "dags-aluno-todo1", dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copy(AIRFLOW_DIR / "gabarito" / "sql" / "preparar_dataset.sql", dag_sql)
    from airflow.models.dagbag import DagBag, sync_bag_to_db
    bag = DagBag(str(TMP / "dags-aluno-todo1"), include_examples=False)
    sync_bag_to_db(bag, "dags-folder", None)
    dag = bag.dags["pipeline_preco_imoveis"]
    preparar(dag)

    estados = rodar(dag)
    assert estados["treinar_e_avaliar"] == "success", estados
    assert estados["registrar_reprovacao"] == "success"  # TODO 3: reprova tudo
    assert estados["publicar"] == "skipped"
    assert "tabela" not in run()["params"]  # TODO 2: run sem rastreabilidade
    assert f.endpoints_resumo() == {}
