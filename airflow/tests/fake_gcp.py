"""GCP simulado para testar as DAGs sem tocar na nuvem.

- BigQuery: DuckDB em memória; o SQL da DAG (dialeto BigQuery) é traduzido com sqlglot.
- Cloud Storage: diretório local.
- Vertex AI: Experiments, Model Registry (versões e aliases) e Endpoints em memória.
  O endpoint faz predição DE VERDADE: carrega o model.joblib do "bucket" e chama predict()
  com o array posicional — o mesmo contrato do container sklearn-cpu.

As regras que derrubam o fluxo real também são aplicadas aqui: alias único por modelo,
não apagar modelo/endpoint com versão implantada, não retirar a versão com tráfego sem
redistribuir, run duplicado no Experiments, parâmetro None no log_params etc.
"""

from __future__ import annotations

import hashlib
import itertools
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import joblib
import numpy as np
import pandas as pd
import sqlglot


# ---------------------------------------------------------------------------
# Estado compartilhado
# ---------------------------------------------------------------------------
@dataclass
class Estado:
    raiz: Path
    projeto: str = "proj-teste"
    regiao: str = "us-central1"
    duck: duckdb.DuckDBPyConnection = None
    modelos: dict = field(default_factory=dict)
    endpoints: dict = field(default_factory=dict)
    runs: dict = field(default_factory=dict)
    run_atual: str | None = None
    experimento: str | None = None
    ids: itertools.count = field(default_factory=lambda: itertools.count(1000))

    def caminho_gcs(self, uri: str) -> Path:
        bucket, _, chave = uri.removeprefix("gs://").partition("/")
        return self.raiz / "gcs" / bucket / chave


ESTADO: Estado | None = None


def novo_estado(raiz: Path, gold: pd.DataFrame) -> Estado:
    global ESTADO
    if raiz.exists():
        shutil.rmtree(raiz)
    raiz.mkdir(parents=True)
    duck = duckdb.connect()
    duck.create_function("farm_fingerprint", _farm_fingerprint, ["VARCHAR"], "BIGINT")
    duck.execute("CREATE SCHEMA aula_pdm")
    duck.register("gold_df", gold)
    duck.execute("CREATE TABLE aula_pdm.imoveis_gold AS SELECT * FROM gold_df")
    duck.unregister("gold_df")
    ESTADO = Estado(raiz=raiz, duck=duck)
    return ESTADO


def _farm_fingerprint(texto: str) -> int:
    return int.from_bytes(hashlib.sha256(texto.encode()).digest()[:8], "big", signed=True)


# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------
def executar_sql(sql: str) -> pd.DataFrame | None:
    # `projeto.dataset.tabela` -> dataset.tabela (o DuckDB tem um "projeto" so)
    sql = re.sub(r"`[^`.]+\.([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)`", r"\1.\2", sql)
    resultado = None
    for comando in sqlglot.transpile(sql, read="bigquery", write="duckdb"):
        cursor = ESTADO.duck.execute(comando)
        if cursor.description:
            resultado = cursor.df()
    return resultado


class _Job:
    def __init__(self, df):
        self._df = df

    def to_dataframe(self, create_bqstorage_client=True, **_):
        # como na VM: a SA nao tem bigquery.readSessionUser, so a API REST funciona
        if create_bqstorage_client is not False:
            raise PermissionError("403 bigquery.readsessions.create (use create_bqstorage_client=False)")
        return self._df

    def result(self):
        return self._df


@dataclass
class _TabelaRef:
    table_id: str

    @property
    def reference(self):
        return self


class FakeBigQueryClient:
    def __init__(self, project=None, **_):
        self.project = project

    def query(self, sql, **_):
        return _Job(executar_sql(sql))

    def list_tables(self, dataset):
        nome = dataset.split(".")[-1]
        linhas = ESTADO.duck.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", [nome]
        ).fetchall()
        return [_TabelaRef(t) for (t,) in linhas]

    def delete_table(self, ref, **_):
        ESTADO.duck.execute(f"DROP TABLE aula_pdm.{ref.table_id}")


def tabelas() -> list[str]:
    return sorted(t.table_id for t in FakeBigQueryClient().list_tables("aula_pdm"))


# ---------------------------------------------------------------------------
# Cloud Storage
# ---------------------------------------------------------------------------
class _Blob:
    def __init__(self, bucket, nome):
        self.bucket, self.name = bucket, nome

    @property
    def _arquivo(self):
        return ESTADO.raiz / "gcs" / self.bucket / self.name

    def upload_from_filename(self, origem):
        self._arquivo.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(origem, self._arquivo)

    def download_to_filename(self, destino):
        if not self._arquivo.exists():
            raise FileNotFoundError(f"gs://{self.bucket}/{self.name}")
        shutil.copy(self._arquivo, destino)

    def delete(self):
        self._arquivo.unlink()


class _Bucket:
    def __init__(self, nome):
        self.name = nome

    def blob(self, nome):
        return _Blob(self.name, nome)

    def list_blobs(self, prefix=""):
        base = ESTADO.raiz / "gcs" / self.name
        if not base.exists():
            return []
        return [_Blob(self.name, str(p.relative_to(base))) for p in sorted(base.rglob("*"))
                if p.is_file() and str(p.relative_to(base)).startswith(prefix)]


class FakeStorageClient:
    def __init__(self, project=None, **_):
        self.project = project

    def bucket(self, nome):
        return _Bucket(nome)


def objetos(prefixo="") -> list[str]:
    base = ESTADO.raiz / "gcs"
    return sorted(str(p.relative_to(base)) for p in base.rglob("*") if p.is_file()
                  and str(p.relative_to(base)).startswith(prefixo)) if base.exists() else []


# ---------------------------------------------------------------------------
# Vertex AI — Experiments
# ---------------------------------------------------------------------------
def init(project=None, location=None, experiment=None, experiment_tensorboard=None, **_):
    if experiment is not None:
        if experiment_tensorboard is not False:
            raise AssertionError("aiplatform.init sem experiment_tensorboard=False criaria TensorBoard pago")
        ESTADO.experimento = experiment


def start_run(nome, resume=False):
    if ESTADO.experimento is None:
        raise RuntimeError("start_run sem experimento no aiplatform.init")
    if nome in ESTADO.runs and not resume:
        raise RuntimeError(f"Run {nome} ja existe")
    if nome not in ESTADO.runs and resume:
        raise RuntimeError(f"Run {nome} nao existe para resume")
    ESTADO.runs.setdefault(nome, {"params": {}, "metrics": {}})
    ESTADO.run_atual = nome


def _checar_escalares(d):
    for k, v in d.items():
        if not isinstance(v, (str, int, float)) or isinstance(v, bool):
            raise TypeError(f"{k}={v!r}: Experiments so aceita str, int ou float")


def log_params(params):
    _checar_escalares(params)
    ESTADO.runs[ESTADO.run_atual]["params"].update(params)


def log_metrics(metricas):
    _checar_escalares(metricas)
    ESTADO.runs[ESTADO.run_atual]["metrics"].update(metricas)


def end_run():
    ESTADO.run_atual = None


# ---------------------------------------------------------------------------
# Vertex AI — Model Registry
# ---------------------------------------------------------------------------
def _nome_modelo(mid):
    return f"projects/{ESTADO.projeto}/locations/{ESTADO.regiao}/models/{mid}"


@dataclass
class _VersionInfo:
    version_id: str
    version_aliases: list


class FakeRegistry:
    def __init__(self, mid):
        self.mid = mid

    @property
    def _m(self):
        return ESTADO.modelos[self.mid]

    def add_version_aliases(self, new_aliases, version):
        for alias in new_aliases:
            dono = [v for v, d in self._m["versoes"].items() if alias in d["aliases"]]
            if dono and dono[0] != str(version):
                raise RuntimeError(f"Alias {alias} ja pertence a versao {dono[0]}")
            self._m["versoes"][str(version)]["aliases"].add(alias)

    def remove_version_aliases(self, target_aliases, version):
        for alias in target_aliases:
            self._m["versoes"][str(version)]["aliases"].discard(alias)

    def list_versions(self):
        return [_VersionInfo(v, sorted(d["aliases"])) for v, d in self._m["versoes"].items()]

    def delete_version(self, version):
        d = self._m["versoes"][str(version)]
        if "default" in d["aliases"]:
            raise RuntimeError("Nao se apaga a versao default")
        if _implantada(self.mid, str(version)):
            raise RuntimeError("Versao implantada nao pode ser apagada")
        del self._m["versoes"][str(version)]


def _implantada(mid, versao=None):
    for ep in ESTADO.endpoints.values():
        for dep in ep["deployed"].values():
            if dep["mid"] == mid and (versao is None or dep["versao"] == versao):
                return True
    return False


class FakeModel:
    def __init__(self, model_name, **_):
        nome, _, sel = model_name.partition("@")
        self.mid = nome.rsplit("/", 1)[-1]
        if self.mid not in ESTADO.modelos:
            raise LookupError(f"Modelo {nome} nao existe")
        m = ESTADO.modelos[self.mid]
        if not sel:
            sel = "default"
        if sel in m["versoes"]:
            self.version_id = sel
        else:
            donos = [v for v, d in m["versoes"].items() if sel in d["aliases"]]
            if not donos:
                raise LookupError(f"404 alias/versao {sel} nao existe")
            self.version_id = donos[0]

    @property
    def _v(self):
        return ESTADO.modelos[self.mid]["versoes"][self.version_id]

    resource_name = property(lambda s: _nome_modelo(s.mid))
    versioned_resource_name = property(lambda s: f"{_nome_modelo(s.mid)}@{s.version_id}")
    display_name = property(lambda s: ESTADO.modelos[s.mid]["display_name"])
    uri = property(lambda s: s._v["artifact_uri"])
    labels = property(lambda s: s._v["labels"])
    version_aliases = property(lambda s: sorted(s._v["aliases"]))
    versioning_registry = property(lambda s: FakeRegistry(s.mid))

    @classmethod
    def list(cls, filter=None, **_):
        nome = re.search(r'display_name="([^"]+)"', filter or "")
        return [cls(_nome_modelo(mid)) for mid, m in ESTADO.modelos.items()
                if not nome or m["display_name"] == nome.group(1)]

    @classmethod
    def upload(cls, display_name, artifact_uri, serving_container_image_uri, parent_model=None,
               is_default_version=True, labels=None, version_description=None, sync=True, **_):
        if not artifact_uri.endswith("/"):
            raise ValueError("artifact_uri deve ser o DIRETORIO (terminar em /)")
        if not ESTADO.caminho_gcs(artifact_uri + "model.joblib").exists():
            raise FileNotFoundError(f"{artifact_uri}model.joblib nao existe")
        if "sklearn-cpu.1-6" not in serving_container_image_uri:
            raise ValueError("container de serving diferente de sklearn-cpu.1-6")
        if parent_model is None:
            if not is_default_version:
                raise ValueError("primeira versao precisa ser default")
            mid = str(next(ESTADO.ids))
            ESTADO.modelos[mid] = {"display_name": display_name, "versoes": {}}
        else:
            mid = parent_model.rsplit("/", 1)[-1]
        versoes = ESTADO.modelos[mid]["versoes"]
        vid = str(len(versoes) + 1 if not versoes else max(map(int, versoes)) + 1)
        versoes[vid] = {"artifact_uri": artifact_uri, "labels": dict(labels or {}),
                        "descricao": version_description, "aliases": set()}
        if is_default_version:
            for d in versoes.values():
                d["aliases"].discard("default")
            versoes[vid]["aliases"].add("default")
        return cls(f"{_nome_modelo(mid)}@{vid}")

    def deploy(self, endpoint, traffic_percentage=100, machine_type=None, min_replica_count=1,
               max_replica_count=1, sync=True, **_):
        ep = ESTADO.endpoints[endpoint.eid]
        dep = str(next(ESTADO.ids))
        ep["deployed"][dep] = {"mid": self.mid, "versao": self.version_id, "maquina": machine_type}
        resto = 100 - traffic_percentage
        for outro in ep["traffic"]:
            ep["traffic"][outro] = 0 if resto == 0 else ep["traffic"][outro]
        ep["traffic"][dep] = traffic_percentage

    def delete(self, sync=True):
        if _implantada(self.mid):
            raise RuntimeError("Modelo implantado nao pode ser apagado: faca undeploy antes")
        del ESTADO.modelos[self.mid]


def modelos_resumo():
    return {m["display_name"]: {v: sorted(d["aliases"]) for v, d in m["versoes"].items()}
            for m in ESTADO.modelos.values()}


# ---------------------------------------------------------------------------
# Vertex AI — Endpoints
# ---------------------------------------------------------------------------
@dataclass
class _Deployed:
    id: str
    model: str
    model_version_id: str


@dataclass
class _Prediction:
    predictions: list
    deployed_model_id: str
    model_version_id: str
    model_resource_name: str


class FakeEndpoint:
    def __init__(self, endpoint_name, **_):
        self.eid = endpoint_name.rsplit("/", 1)[-1]
        if self.eid not in ESTADO.endpoints:
            raise LookupError(f"Endpoint {endpoint_name} nao existe")

    resource_name = property(lambda s: f"projects/{ESTADO.projeto}/locations/{ESTADO.regiao}/endpoints/{s.eid}")

    @property
    def _e(self):
        return ESTADO.endpoints[self.eid]

    @classmethod
    def list(cls, filter=None, **_):
        nome = re.search(r'display_name="([^"]+)"', filter or "")
        return [cls(eid) for eid, e in ESTADO.endpoints.items()
                if not nome or e["display_name"] == nome.group(1)]

    @classmethod
    def create(cls, display_name, **_):
        eid = str(next(ESTADO.ids))
        ESTADO.endpoints[eid] = {"display_name": display_name, "deployed": {}, "traffic": {}}
        return cls(eid)

    def list_models(self):
        return [_Deployed(d, _nome_modelo(v["mid"]), v["versao"]) for d, v in self._e["deployed"].items()]

    def predict(self, instances):
        if not self._e["deployed"]:
            raise RuntimeError("Endpoint sem modelo implantado")
        dep = max(self._e["traffic"], key=self._e["traffic"].get)
        info = self._e["deployed"][dep]
        uri = ESTADO.modelos[info["mid"]]["versoes"][info["versao"]]["artifact_uri"]
        pipe = joblib.load(ESTADO.caminho_gcs(uri + "model.joblib"))
        # o container entrega as instances ao predict como array posicional
        preds = pipe.predict(np.asarray(instances, dtype=float)).tolist()
        return _Prediction(preds, dep, info["versao"], _nome_modelo(info["mid"]))

    def undeploy(self, deployed_model_id, traffic_split=None, sync=True):
        e = self._e
        if deployed_model_id not in e["deployed"]:
            raise LookupError(f"deployment {deployed_model_id} nao existe")
        restantes = [d for d in e["deployed"] if d != deployed_model_id]
        if traffic_split is None:
            if e["traffic"][deployed_model_id] > 0 and restantes:
                raise ValueError("Undeploy deixaria o trafego dos restantes em 0%: passe traffic_split")
            novo = {d: e["traffic"][d] for d in restantes}
        else:
            if set(traffic_split) != set(restantes) or sum(traffic_split.values()) != 100:
                raise ValueError(f"traffic_split invalido: {traffic_split}")
            novo = dict(traffic_split)
        del e["deployed"][deployed_model_id]
        e["traffic"] = novo

    def undeploy_all(self, sync=True):
        # como o SDK: primeiro os sem trafego, por ultimo o que recebe 100%
        for d in sorted(self._e["deployed"], key=lambda d: self._e["traffic"][d]):
            self.undeploy(d)

    def delete(self, sync=True, force=False):
        if self._e["deployed"]:
            raise RuntimeError("Endpoint com modelo implantado nao pode ser apagado")
        del ESTADO.endpoints[self.eid]


def endpoints_resumo():
    return {e["display_name"]: {d: (v["versao"], e["traffic"][d]) for d, v in e["deployed"].items()}
            for e in ESTADO.endpoints.values()}


# ---------------------------------------------------------------------------
# Instalacao dos fakes
# ---------------------------------------------------------------------------
def instalar(monkeypatch):
    from airflow.exceptions import AirflowException
    from airflow.providers.google.cloud.operators.bigquery import (
        BigQueryCheckOperator,
        BigQueryInsertJobOperator,
    )
    from google.cloud import aiplatform, bigquery, storage

    monkeypatch.setattr(bigquery, "Client", FakeBigQueryClient)
    monkeypatch.setattr(storage, "Client", FakeStorageClient)
    for nome, obj in {"init": init, "start_run": start_run, "log_params": log_params,
                      "log_metrics": log_metrics, "end_run": end_run,
                      "Model": FakeModel, "Endpoint": FakeEndpoint}.items():
        monkeypatch.setattr(aiplatform, nome, obj)

    def insert_job(self, context):
        executar_sql(self.configuration["query"]["query"])

    def check(self, context):
        df = executar_sql(self.sql)
        linha = df.iloc[0].to_dict()
        falsas = [k for k, v in linha.items() if not bool(v)]
        if falsas:
            raise AirflowException(f"Checagem falhou: {falsas}")

    monkeypatch.setattr(BigQueryInsertJobOperator, "execute", insert_job)
    monkeypatch.setattr(BigQueryCheckOperator, "execute", check)
