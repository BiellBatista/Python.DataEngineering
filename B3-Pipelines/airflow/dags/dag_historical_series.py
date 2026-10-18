"""DAG ELT da B3 com Data Quality entre as camadas do pipeline.

Valida a largura de 245 bytes e os tipos de registro do RAW definidos no
manual oficial COTAHIST, valida chaves e preços na Bronze e só então carrega
a Silver. O preço chamado fechamento pelo pipeline usa o campo PREULT do
manual (preço do último negócio do pregão). Consulte o
`layout oficial da B3 <https://www.b3.com.br/data/files/C8/F3/08/B4/297BE410F816C9E492D828A8/SeriesHistoricas_Layout.pdf>`_.
"""

from datetime import datetime, timedelta
from typing import Dict, Any
from airflow.sdk import dag, task, Param
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
    """Define validações de qualidade entre RAW, Bronze e Silver."""

    @task(task_id="validate_raw_data_quality")
    def check_raw_step(**context: Any) -> Dict[str, Any]:
        """Valida os arquivos RAW antes de permitir a carga Bronze.

        Args:
            **context: Contexto da task, incluindo os parâmetros da DAG.

        Returns:
            Intervalo de anos validado, para a carga Bronze subsequente.
        """
        params = context["params"]
        start_year = int(params["start_year"])
        end_year = int(params["end_year"])

        print(
            f"Validando arquivos RAW para o período: "
            f"{start_year} a {end_year}"
        )
        try:
            validate_raw_quality(start_year=start_year, end_year=end_year)
        except DataQualityError as exc:
            raise AirflowFailException(str(exc)) from exc
        print("Data Quality RAW aprovada.")
        return {"start_year": start_year, "end_year": end_year}

    @task(task_id="load_bronze_layer")
    def run_bronze_step(
        raw_metadata: Dict[str, Any],
        run_id: str,
        logical_date: datetime,
        **context: Any,
    ) -> Dict[str, Any]:
        """Carrega os arquivos RAW na Bronze e retorna metadados leves.

        Args:
            raw_metadata: Intervalo validado pela task de qualidade RAW.
            run_id: Identificador da execução fornecido pelo Airflow.
            logical_date: Data lógica da DAG, reutilizada em retries.
            **context: Contexto da task, incluindo os parâmetros da DAG.

        Returns:
            Anos processados e estado da carga, apropriados para XCom.
        """
        params = context["params"]
        start_year = raw_metadata["start_year"]
        end_year = raw_metadata["end_year"]
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

    @task(task_id="validate_bronze_data_quality")
    def check_bronze_step(
        bronze_metadata: Dict[str, Any],
        **context: Any,
    ) -> Dict[str, Any]:
        """Valida os registros Bronze antes de permitir a carga Silver.

        Args:
            bronze_metadata: Intervalo de anos carregado na Bronze.
            **context: Contexto da task, incluindo os parâmetros da DAG.

        Returns:
            Os metadados, após a aprovação das regras de qualidade.
        """
        params = context["params"]
        start_year = bronze_metadata["start_year"]
        end_year = bronze_metadata["end_year"]

        print(
            f"Validando dados Bronze para o período: "
            f"{start_year} a {end_year}"
        )
        try:
            validate_bronze_quality(
                start_year=start_year,
                end_year=end_year,
                memory_limit=str(params["memory_limit"]),
                threads=int(params["threads"]),
            )
        except DataQualityError as exc:
            raise AirflowFailException(str(exc)) from exc
        print("Data Quality Bronze aprovada.")
        return bronze_metadata

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

    raw_metadata = check_raw_step()
    bronze_output = run_bronze_step(raw_metadata=raw_metadata)
    validated_bronze = check_bronze_step(bronze_metadata=bronze_output)
    run_silver_step(bronze_metadata=validated_bronze)


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
