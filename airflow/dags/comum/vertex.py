"""Auxiliares do Vertex AI usados pelas DAGs (Registry, aliases e campeao atual)."""

import logging
import tempfile

import joblib
from google.cloud import aiplatform, storage

from comum import config as cfg

log = logging.getLogger(__name__)


def iniciar(experimento=False):
    if experimento:
        # experiment_tensorboard=False: sem ele o SDK cria uma TensorBoard paga sozinho.
        aiplatform.init(project=cfg.PROJETO, location=cfg.REGIAO,
                        experiment=cfg.EXPERIMENTO, experiment_tensorboard=False)
    else:
        aiplatform.init(project=cfg.PROJETO, location=cfg.REGIAO)


def modelo_pai():
    """Entrada do modelo no catalogo (uma por display_name), ou None."""
    modelos = aiplatform.Model.list(filter=f'display_name="{cfg.MODELO}"')
    return modelos[0] if modelos else None


def versao_por_alias(alias):
    """Versao do modelo com o alias informado, ou None."""
    pai = modelo_pai()
    if pai is None:
        return None
    try:
        return aiplatform.Model(model_name=f"{pai.resource_name}@{alias}")
    except Exception:  # alias inexistente
        return None


def mover_alias(alias, version_id):
    """Aliases sao unicos por modelo: tira de quem tem e poe na versao indicada."""
    pai = modelo_pai()
    registro = pai.versioning_registry
    atual = versao_por_alias(alias)
    if atual is not None and str(atual.version_id) != str(version_id):
        registro.remove_version_aliases([alias], version=atual.version_id)
    registro.add_version_aliases([alias], version=str(version_id))
    log.info("alias %s -> versao %s", alias, version_id)


def carregar_campeao():
    """(pipeline, version_id) do modelo com alias campeao, ou (None, None)."""
    campeao = versao_por_alias(cfg.ALIAS_CAMPEAO)
    if campeao is None:
        log.info("Ainda nao existe campeao: comparacao so com o baseline.")
        return None, None
    uri = campeao.uri.rstrip("/") + "/model.joblib"
    bucket, _, caminho = uri.removeprefix("gs://").partition("/")
    with tempfile.NamedTemporaryFile(suffix=".joblib") as tmp:
        storage.Client(project=cfg.PROJETO).bucket(bucket).blob(caminho).download_to_filename(tmp.name)
        pipe = joblib.load(tmp.name)
    log.info("Campeao atual: versao %s (%s)", campeao.version_id, uri)
    return pipe, campeao.version_id
