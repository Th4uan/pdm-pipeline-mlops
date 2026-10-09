"""Contrato de nomes da aula — o mesmo do notebook e dos scripts.

Projeto, bucket e gold chegam como variaveis de ambiente AIRFLOW_VAR_* (geradas na VM
a partir dos metadados da instancia). Assim a mesma DAG roda em qualquer projeto sem edicao.
"""

import os

PROJETO = os.environ.get("AIRFLOW_VAR_PROJETO", "")
REGIAO = os.environ.get("AIRFLOW_VAR_REGIAO", "us-central1")
BUCKET = os.environ.get("AIRFLOW_VAR_BUCKET", f"{PROJETO}-mlops-aula")
DATASET = "aula_pdm"
GOLD_TABLE = os.environ.get("AIRFLOW_VAR_GOLD_TABLE", f"{PROJETO}.{DATASET}.imoveis_gold")

EXPERIMENTO = "preco-imoveis-rf"
MODELO = "rf-preco-imoveis"
ENDPOINT = "rf-preco-imoveis-endpoint"
SERVING_CONTAINER = "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-6:latest"
MAQUINA_ENDPOINT = "n1-standard-2"

# ORDEM CANONICA — contrato de entrada do modelo e do payload do endpoint.
FEATURES = ["area_util", "area_total", "quartos", "banheiros", "garagens"]
ALVO = "preco"
CHAVE_IMOVEL = "id"

# Payload usado no teste de fumaca do endpoint: [area_util, area_total, quartos, banheiros, garagens]
PAYLOAD_REFERENCIA = [[120.0, 150.0, 3, 2, 1]]
FAIXA_PLAUSIVEL = (10_000, 50_000_000)  # mesmos limites de preco da gold

ALIAS_CANDIDATO = "candidato"
ALIAS_CAMPEAO = "campeao"
