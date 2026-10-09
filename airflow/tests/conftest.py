"""Ambiente de teste: Airflow com SQLite temporario + GCP simulado (fake_gcp.py).

Rode com:  bash tests/rodar_testes.sh   (a partir de airflow/)
"""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

AIRFLOW_DIR = Path(__file__).resolve().parents[1]
TMP = Path(tempfile.mkdtemp(prefix="airflow-aula-"))

# Variaveis lidas pelas DAGs (na VM vem do startup-script.sh)
os.environ.update({
    "AIRFLOW_HOME": str(TMP / "home"),
    "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{TMP / 'airflow.db'}",
    "AIRFLOW__CORE__DAGS_FOLDER": str(TMP / "dags"),
    "AIRFLOW_VAR_PROJETO": "proj-teste",
    "AIRFLOW_VAR_REGIAO": "us-central1",
    "AIRFLOW_VAR_BUCKET": "proj-teste-mlops-aula",
    "AIRFLOW_VAR_GOLD_TABLE": "proj-teste.aula_pdm.imoveis_gold",
})
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(AIRFLOW_DIR / "dags"))  # na VM: PYTHONPATH=/opt/airflow/dags

subprocess.run([sys.executable, "-m", "airflow", "db", "migrate"], check=True,
               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def gold_sintetica(n=1500, semente=7):
    """Mesmas colunas da gold do seed_gold.sh, com preco dependente das features."""
    import numpy as np
    import pandas as pd

    rng = np.random.default_rng(semente)
    area_util = rng.uniform(35, 400, n).round(1)
    quartos = rng.integers(1, 6, n)
    banheiros = np.clip(quartos - rng.integers(0, 2, n), 1, None)
    garagens = rng.integers(0, 4, n).astype(float)
    garagens[rng.random(n) < 0.05] = np.nan  # buracos, como na gold real
    preco = 4500 * area_util + 40000 * quartos + 25000 * np.nan_to_num(garagens) + rng.normal(0, 60000, n)
    return pd.DataFrame({
        "id": np.arange(1, n + 1),
        "preco": np.clip(preco, 50000, None).round(2),
        "area_util": area_util,
        "area_total": (area_util * rng.uniform(1.0, 1.4, n)).round(1),
        "quartos": quartos,
        "banheiros": banheiros,
        "garagens": garagens,
        "suites": np.clip(quartos - 2, 0, None),
        "bairro": rng.choice(["Centro", "Setor Bueno", "Setor Oeste", "Jardim Goias"], n),
        "cidade": "Goiania",
        "estado": "GO",
    })


def carregar_dag(dag_id, gabarito):
    """Monta a pasta dags/ como o 20_sync_dags.sh enviaria (com ou sem gabarito) e carrega a DAG."""
    from airflow.dag_processing.bundles.manager import DagBundlesManager
    from airflow.models.dagbag import DagBag, sync_bag_to_db

    destino = TMP / ("dags-gabarito" if gabarito else "dags-aluno")
    if destino.exists():
        shutil.rmtree(destino)
    shutil.copytree(AIRFLOW_DIR / "dags", destino, ignore=shutil.ignore_patterns("__pycache__"))
    if gabarito:
        shutil.copytree(AIRFLOW_DIR / "gabarito", destino, dirs_exist_ok=True)
    bag = DagBag(str(destino), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    DagBundlesManager().sync_bundles_to_db()
    sync_bag_to_db(bag, "dags-folder", None)  # Airflow 3: dag.test exige a DAG serializada
    dag = bag.dags[dag_id]
    preparar(dag)
    return dag


def preparar(dag):
    for t in dag.tasks:
        t.retries = 0  # no teste uma falha e' final; na VM ha retries
    # dag.test reaproveita o mesmo objeto DAG no processo: guarda os defaults dos params
    # para que o conf de uma execucao nao vaze para a seguinte (na VM cada run e' isolado)
    dag.padroes_params = {k: dag.params[k] for k in dag.params}


@pytest.fixture
def gcp(monkeypatch):
    import fake_gcp

    estado = fake_gcp.novo_estado(TMP / "gcp", gold_sintetica())
    fake_gcp.instalar(monkeypatch)
    return estado


def rodar(dag, **conf):
    """Executa a DAG no proprio processo e devolve {task_id: estado}."""
    dr = dag.test(run_conf={**dag.padroes_params, **conf})
    return {ti.task_id: str(ti.state) for ti in dr.get_task_instances()}
