"""Ponto de entrada para executar sequencialmente as cargas Bronze e Silver."""

from datetime import datetime, timezone

from config import (
    BRONZE_MAX_WORKERS,
    DEFAULT_END_YEAR,
    DEFAULT_START_YEAR,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    MANUAL_RUN_ID_PREFIX,
    RUN_ID_TIMESTAMP_FORMAT,
)
from historical_series.transformations.bronze import execute_bronze_load
from historical_series.transformations.silver import execute_silver_load


def main() -> None:
    """Executa as cargas Bronze e Silver com os padrões do ambiente."""
    ingestion_time = datetime.now(timezone.utc)
    dag_run_id = (
        f"{MANUAL_RUN_ID_PREFIX}-"
        f"{ingestion_time.strftime(RUN_ID_TIMESTAMP_FORMAT)}"
    )

    execute_bronze_load(
        dag_run_id,
        DEFAULT_START_YEAR,
        DEFAULT_END_YEAR,
        memory_limit=DUCKDB_MEMORY_LIMIT,
        threads=DUCKDB_THREADS,
        max_workers=BRONZE_MAX_WORKERS,
        ingestion_time=ingestion_time,
    )
    execute_silver_load(
        dag_run_id,
        DEFAULT_START_YEAR,
        DEFAULT_END_YEAR,
        memory_limit=DUCKDB_MEMORY_LIMIT,
        threads=DUCKDB_THREADS,
    )


if __name__ == "__main__":
    main()
