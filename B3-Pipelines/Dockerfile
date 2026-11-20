ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.1-python3.12
FROM ${AIRFLOW_BASE_IMAGE}

COPY --chown=airflow:root pyproject.toml ./
COPY --chown=airflow:root src ./src

RUN pip install --no-cache-dir \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt \
    "apache-airflow[otel]==3.3.1" \
    .