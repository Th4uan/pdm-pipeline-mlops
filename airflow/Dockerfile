# Imagem do Airflow da aula: a oficial (que ja traz o provider Google e o
# google-cloud-aiplatform) + scikit-learn na mesma versao do container de serving.
ARG AIRFLOW_IMAGE_BASE=apache/airflow:3.1.0-python3.12
FROM ${AIRFLOW_IMAGE_BASE}

COPY requirements.txt /requirements.txt
# Fixar apache-airflow na mesma instalacao impede que o pip troque a versao do Airflow.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
