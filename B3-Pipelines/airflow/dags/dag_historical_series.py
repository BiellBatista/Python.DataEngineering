"""DAG ELT da B3 com Data Quality entre as camadas do pipeline.

Para cada ano do intervalo, valida o RAW, carrega Bronze, valida regras de
negócio e atualiza somente a partição Silver daquele ano. Uma falha num ano
não impede a execução independente dos outros. A validação de RAW verifica
largura de 245 bytes e tipos de registro conforme o manual COTAHIST; o preço
chamado fechamento pelo pipeline usa o campo PREULT (último negócio). Consulte o
`layout oficial da B3 <https://www.b3.com.br/data/files/C8/F3/08/B4/297BE410F816C9E492D828A8/SeriesHistoricas_Layout.pdf>`_.
"""

from datetime import datetime, timedelta
from typing import Any
from airflow.sdk import dag, task, task_group, Param
from airflow.sdk.exceptions import AirflowFailException

from config import (
    BRONZE_MAX_WORKERS,
    DEFAULT_END_YEAR,
    DEFAULT_START_YEAR,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
)
from historical_series.data_qualities import (
    DataQualityError,
    validate_bronze_quality,
    validate_raw_quality,
)
from historical_series.transformations.bronze import execute_bronze_load
from historical_series.transformations.silver import execute_silver_load

@dag(
    dag_id="b3_cotahist_medallion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Execução manual/sob demanda
    catchup=False,
    max_active_runs=1,
    max_active_tasks=BRONZE_MAX_WORKERS,
    default_args={
        "owner": "data_engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    # Parâmetros com validação JSON Schema para interface gráfica do Airflow
    params={
        "start_year": Param(
            DEFAULT_START_YEAR,
            type="integer",
            minimum=1986,
            maximum=2030,
            title="Ano Inicial",
            description="Ano inicial do lote da série histórica da B3.",
        ),
        "end_year": Param(
            DEFAULT_END_YEAR,
            type="integer",
            minimum=1986,
            maximum=2030,
            title="Ano Final",
            description="Ano final do lote da série histórica da B3.",
        ),
        "memory_limit": Param(
            DUCKDB_MEMORY_LIMIT,
            type="string",
            title="Limite de Memória DuckDB",
            description="Limite de memória RAM por worker DuckDB (ex: '2GB', '4GB').",
        ),
        "threads": Param(
            DUCKDB_THREADS,
            type="integer",
            minimum=1,
            title="Threads DuckDB",
            description="Quantidade de threads DuckDB utilizadas em cada worker.",
        ),
    },
    tags=["b3", "market_data", "duckdb", "medallion", "cotahist"],
    doc_md=__doc__,
)
def b3_cotahist_pipeline():
    """Define o processamento independente por ano e suas barreiras DQ."""

    @task
    def get_years_to_process(**context: Any) -> list[int]:
        """Retorna os anos do intervalo solicitado para mapeamento dinâmico.

        Args:
            **context: Contexto da task, incluindo os parâmetros da DAG.

        Returns:
            Anos a serem processados independentemente.
        """
        params = context["params"]
        start_year = int(params["start_year"])
        end_year = int(params["end_year"])
        if start_year > end_year:
            raise ValueError("start_year deve ser menor ou igual a end_year.")
        years = list(range(start_year, end_year + 1))
        print(f"Anos agendados para processamento independente: {years}")
        return years

    @task_group(group_id="process_year")
    def process_year(year: int) -> None:
        """Executa as etapas do pipeline isoladas para um ano."""

        @task(task_id="validate_raw_data_quality")
        def check_raw_step(year: int) -> int:
            """Valida o RAW do ano antes de permitir a carga Bronze."""
            print(f"[{year}] Validando arquivo RAW.")
            try:
                validate_raw_quality(start_year=year, end_year=year)
            except DataQualityError as exc:
                raise AirflowFailException(str(exc)) from exc
            print(f"[{year}] Data Quality RAW aprovada.")
            return year

        @task(task_id="load_bronze_layer")
        def run_bronze_step(
            year: int,
            run_id: str,
            logical_date: datetime,
            **context: Any,
        ) -> int:
            """Carrega na Bronze somente o RAW do ano validado."""
            params = context["params"]
            print(f"[{year}] Iniciando carga Bronze.")
            execute_bronze_load(
                dag_run_id=run_id,
                start_year=year,
                end_year=year,
                memory_limit=str(params["memory_limit"]),
                threads=int(params["threads"]),
                max_workers=1,
                ingestion_time=logical_date,
            )
            print(f"[{year}] Carga Bronze concluída.")
            return year

        @task(task_id="validate_bronze_data_quality")
        def check_bronze_step(year: int, **context: Any) -> int:
            """Valida na Bronze as regras de qualidade para o ano."""
            params = context["params"]
            print(f"[{year}] Validando dados Bronze.")
            try:
                validate_bronze_quality(
                    start_year=year,
                    end_year=year,
                    memory_limit=str(params["memory_limit"]),
                    threads=int(params["threads"]),
                )
            except DataQualityError as exc:
                raise AirflowFailException(str(exc)) from exc
            print(f"[{year}] Data Quality Bronze aprovada.")
            return year

        @task(task_id="load_silver_layer")
        def run_silver_step(year: int, run_id: str, **context: Any) -> None:
            """Atualiza apenas a partição Silver do ano aprovado."""
            params = context["params"]
            print(f"[{year}] Iniciando carga Silver.")
            execute_silver_load(
                dag_run_id=run_id,
                start_year=year,
                end_year=year,
                memory_limit=str(params["memory_limit"]),
                threads=int(params["threads"]),
            )
            print(f"[{year}] Carga Silver concluída.")

        raw_year = check_raw_step(year)
        bronze_year = run_bronze_step(raw_year)
        quality_year = check_bronze_step(bronze_year)
        run_silver_step(quality_year)

    years = get_years_to_process()
    process_year.expand(year=years)


# Instanciação da DAG no escopo do módulo
pipeline_dag = b3_cotahist_pipeline()

# Bloco para execução e teste local via terminal ou WSL:
# python airflow/dags/dag_b3_cotahist.py
if __name__ == "__main__":
    pipeline_dag.test(
        run_conf={
            "start_year": DEFAULT_START_YEAR,
            "end_year": DEFAULT_END_YEAR,
            "memory_limit": DUCKDB_MEMORY_LIMIT,
            "threads": DUCKDB_THREADS,
        }
    )
