"""
DAG de Orquestração do Pipeline ELT da Série Histórica da B3 (COTAHIST)
Arquitetura Medalhão: RAW -> Bronze -> Silver com DuckDB e Python.
"""

from datetime import datetime, timedelta
from typing import Dict, Any
from airflow.sdk import dag, task, Param

from config import (
    BRONZE_MAX_WORKERS,
    DEFAULT_END_YEAR,
    DEFAULT_START_YEAR,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
)
from historical_series.transformations.bronze import execute_bronze_load
from historical_series.transformations.silver import execute_silver_load

@dag(
    dag_id="b3_cotahist_medallion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Execução manual/sob demanda
    catchup=False,
    max_active_runs=1,
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
        "max_workers": Param(
            BRONZE_MAX_WORKERS,
            type="integer",
            minimum=1,
            maximum=16,
            title="Workers Simultâneos",
            description="Quantidade de anos/diretórios processados em paralelo.",
        ),
    },
    tags=["b3", "market_data", "duckdb", "medallion", "cotahist"],
    doc_md=__doc__,
)
def b3_cotahist_pipeline():
    """Define o fluxo de ingestão Bronze seguido da transformação Silver."""

    @task(task_id="load_bronze_layer")
    def run_bronze_step(run_id: str, logical_date: datetime, **context: Any) -> Dict[str, Any]:
        """Carrega os arquivos RAW na Bronze e retorna metadados leves.

        Args:
            run_id: Identificador da execução fornecido pelo Airflow.
            logical_date: Data lógica da DAG, reutilizada em retries.
            **context: Contexto da task, incluindo os parâmetros da DAG.

        Returns:
            Anos processados e estado da carga, apropriados para XCom.
        """
        params = context["params"]
        start_year = int(params["start_year"])
        end_year = int(params["end_year"])
        memory_limit = str(params["memory_limit"])
        threads = int(params["threads"])
        max_workers = int(params["max_workers"])

        print(f"[{run_id}] Iniciando Carga Bronze para o período: {start_year} a {end_year}")

        execute_bronze_load(
            dag_run_id=run_id,
            start_year=start_year,
            end_year=end_year,
            memory_limit=memory_limit,
            threads=threads,
            max_workers=max_workers,
            ingestion_time=logical_date,  # Mantém a partição estável em retries
        )

        # Retorno de metadados leves para o XCom
        return {
            "start_year": start_year,
            "end_year": end_year,
            "status": "bronze_completed",
        }

    @task(task_id="load_silver_layer")
    def run_silver_step(bronze_metadata: Dict[str, Any], run_id: str, **context: Any) -> None:
        """Deduplica e grava na Silver os anos informados pela task Bronze.

        Args:
            bronze_metadata: Intervalo de anos retornado pela task Bronze.
            run_id: Identificador da execução fornecido pelo Airflow.
            **context: Contexto da task, incluindo os parâmetros da DAG.
        """
        params = context["params"]
        start_year = bronze_metadata["start_year"]
        end_year = bronze_metadata["end_year"]
        memory_limit = str(params["memory_limit"])
        threads = int(params["threads"])

        print(f"[{run_id}] Iniciando Carga Silver para o período: {start_year} a {end_year}")

        execute_silver_load(
            dag_run_id=run_id,
            start_year=start_year,
            end_year=end_year,
            memory_limit=memory_limit,
            threads=threads,
        )

        print(f"[{run_id}] Carga da camada Silver concluída com sucesso.")

    # Dependência declarativa TaskFlow: a Silver consome o retorno leve da Bronze
    bronze_output = run_bronze_step()
    run_silver_step(bronze_metadata=bronze_output)


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
            "max_workers": BRONZE_MAX_WORKERS,
        }
    )
