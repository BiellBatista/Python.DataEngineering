ARG DUCKDB_BASE_IMAGE=python:3.15-slim
FROM ${DUCKDB_BASE_IMAGE}

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
COPY duckdb-server/server.py ./server.py

RUN pip install --no-cache-dir . \
    && useradd --uid 50000 --gid 0 --create-home --home-dir /home/duckdb duckdb \
    && chmod -R g+rwX /home/duckdb

ENV HOME=/home/duckdb \
    PYTHONUNBUFFERED=1

USER 50000:0

CMD ["python", "server.py"]
