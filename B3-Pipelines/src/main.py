from datetime import datetime, timezone

from historical_series.transformations.bronze import execute_bronze_load
from historical_series.transformations.silver import execute_silver_load


def main() -> None:
    dag_run_id = "teste-123"
    start_year = 2024
    end_year = 2025
    ingestion_time = datetime.now(timezone.utc)

    execute_bronze_load(
        dag_run_id,
        start_year,
        end_year,
        memory_limit="8GB",
        threads=8,
        max_workers=4,
        ingestion_time=ingestion_time,
    )
    execute_silver_load(
        dag_run_id,
        start_year,
        end_year,
        memory_limit="8GB",
        threads=8,
    )


if __name__ == "__main__":
    main()
