FROM apache/airflow:3.3.1-python3.12

COPY --chown=airflow:root pyproject.toml ./
COPY --chown=airflow:root src ./src

RUN pip install --no-cache-dir .