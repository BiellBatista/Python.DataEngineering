"""DAG sob demanda para regenerar apenas a camada Gold de candles.

Executa somente a transformação ``gold/candles`` sobre o dataset Silver já
consolidado, sem repetir as camadas Bronze e Silver. O seletor de ticker
aceita uma lista de prefixos separada por vírgula; vazio processa todos.
"""

from datetime import datetime
from typing import Any
from airflow.sdk import dag, task, Param

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    GOLD_MAX_WORKERS,
)
from historical_series.transformations.gold_candles import (
    execute_gold_candles_load,
)
from utils.helpers import parse_ticker_list


@dag(
    dag_id="b3_gold_candles",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Execução manual/sob demanda
    catchup=False,
    max_active_runs=1,
    params={
        "start_with": Param(
            "",
            type="string",
            title="Ticker começa com",
            description=(
                "Prefixos de tickers separados por vírgula "
                "(ex.: 'VALE, PETR, BBAS'). Deixe vazio para processar todos."
            ),
        ),
        "memory_limit": Param(
            DUCKDB_MEMORY_LIMIT,
            type="string",
            title="Limite de Memória DuckDB",
            description=(
                "Limite global da instância DuckDB. Em Docker, deve corresponder "
                "a DUCKDB_MEMORY_LIMIT no .env."
            ),
        ),
        "threads": Param(
            DUCKDB_THREADS,
            type="integer",
            minimum=1,
            title="Threads DuckDB",
            description=(
                "Limite global de threads da instância DuckDB. Em Docker, deve "
                "corresponder a DUCKDB_THREADS no .env."
            ),
        ),
        "max_workers": Param(
            GOLD_MAX_WORKERS,
            type="integer",
            minimum=1,
            title="Workers Gold",
            description=(
                "Máximo de grupos de tickers processados em paralelo."
            ),
        ),
    },
    tags=["b3", "market_data", "duckdb", "gold", "candles"],
    doc_md=__doc__,
)
def b3_gold_candles_pipeline():
    """Executa a carga Gold de candles sobre a Silver existente."""

    @task(task_id="load_gold_candles")
    def run_gold_candles_step(run_id: str, **context: Any) -> None:
        """Gera as partições Gold a partir do dataset Silver consolidado."""
        params = context["params"]
        print("Iniciando geração dos candles Gold (execução isolada).")
        execute_gold_candles_load(
            start_with=parse_ticker_list(params["start_with"]),
            dag_run_id=run_id,
            memory_limit=str(params["memory_limit"]),
            threads=int(params["threads"]),
            max_workers=int(params["max_workers"]),
        )
        print("Geração dos candles Gold concluída.")

    run_gold_candles_step()


pipeline_dag = b3_gold_candles_pipeline()

if __name__ == "__main__":
    pipeline_dag.test(
        run_conf={
            "start_with": "",
            "memory_limit": DUCKDB_MEMORY_LIMIT,
            "threads": DUCKDB_THREADS,
            "max_workers": GOLD_MAX_WORKERS,
        }
    )
