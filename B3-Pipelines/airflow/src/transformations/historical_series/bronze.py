import duckdb
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from ingestion.historical_series.raw import list_raw_dirs
from typing import Optional
from utils.helpers import ensure_directory_available

logger = logging.getLogger(__name__)

OUTPUT_DIR = "/mnt/d/b3_datalake/bronze/cotahist"
DEFAULT_MAX_WORKERS = 4
# Utilizado r"""...""" para preservar as barras invertidas do RegEx (\d)
COPY_QUERY = r"""
    COPY (
        SELECT
            content,
            $ingestion_time AS _ingested_at,
            filename AS _source_file,
            $dag_run_id AS _dag_run_id,
            -- TRY_CAST evita quebra se algum arquivo não tiver o ano de 4 dígitos no nome
            TRY_CAST(regexp_extract(filename, 'COTAHIST_A(\d{4})', 1) AS INTEGER) AS year,
            strftime($ingestion_time, '%Y-%m-%d_%H-%M-%S') AS _ingested_date
        FROM read_csv(
            $input_dir,
            header=False,
            delim='\n',
            quote='',
            columns={'content': 'VARCHAR'},
            filename=True,
            parallel=True
        )
    ) TO $output_dir (
        FORMAT PARQUET,
        PARTITION_BY (year, _ingested_date),
        COMPRESSION 'ZSTD',
        ROW_GROUP_SIZE 122880,
        OVERWRITE_OR_IGNORE
    );
"""

def execute_bronze_load(
    dag_run_id: str,
    start_year: int,
    end_year: int,
    memory_limit: Optional[str] = None,
    threads: Optional[int] = None,
    max_workers: Optional[int] = None,
    input_dir: str = "/mnt/d/b3_datalake/raw/cotahist",
    output_dir: str = OUTPUT_DIR,
    ingestion_time: Optional[datetime] = None,
) -> None:
    """
    Executa a carga Bronze de forma paralela.

    Parâmetros
    ----------
    dag_run_id:
        Identificador da execução do pipeline.

    start_year / end_year:
        Intervalo utilizado para localizar os diretórios RAW.

    memory_limit:
        Limite de memória POR WORKER DuckDB.
        Ex.: '2GB'.

    threads:
        Número TOTAL de threads DuckDB desejado.
        Essas threads serão distribuídas entre os workers.

    max_workers:
        Número máximo de raw_dirs processados simultaneamente.
        Se omitido, utiliza no máximo 4 workers.

    input_dir / output_dir:
        Diretórios base das camadas RAW e Bronze.

    ingestion_time:
        Timestamp lógico da carga. Para retries Airflow idempotentes, passe
        o mesmo timestamp lógico da DAG em todas as tentativas.
    """

    if start_year > end_year:
        raise ValueError("start_year deve ser menor ou igual a end_year.")
    if threads is not None and threads <= 0:
        raise ValueError("threads deve ser maior que zero.")
    if max_workers is not None and max_workers <= 0:
        raise ValueError("max_workers deve ser maior que zero.")
    _validate_memory_limit(memory_limit)
    effective_ingestion_time = ingestion_time or datetime.now(timezone.utc)

    try:
        ensure_directory_available(output_dir)

        logger.info("Iniciando carga da Camada Bronze...")

        raw_dirs = list_raw_dirs(start_year, end_year, input_dir)

        if not raw_dirs:
            raise FileNotFoundError("Nenhum arquivo RAW encontrado para processar.")

        total_threads = threads if threads is not None else (os.cpu_count() or 1)
        worker_limit = max_workers if max_workers is not None else DEFAULT_MAX_WORKERS
        workers = min(worker_limit, len(raw_dirs), total_threads)
        # Divide o orçamento total de threads
        # entre os workers.
        threads_per_worker = max(1, total_threads // workers)

        logger.info(f"RAW dirs encontrados: {len(raw_dirs)}")
        logger.info(f"Workers paralelos: {workers}")
        logger.info(f"Threads DuckDB por worker: {threads_per_worker}")
        logger.info(f"Threads totais máximas: {workers * threads_per_worker}")
        logger.info(f"Destino: {output_dir}")

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="bronze-worker") as executor:
            futures = {
                executor.submit(
                    _load_raw_dir,
                    raw_dir,
                    dag_run_id,
                    output_dir,
                    memory_limit,
                    threads_per_worker,
                    effective_ingestion_time,
                ): raw_dir
                for raw_dir in raw_dirs
            }
            completed = 0

            for future in as_completed(futures):
                # Se algum worker falhar, a exceção é
                # propagada para esta função.
                future.result()
                completed += 1

                logger.info(f"Progresso: {completed}/{len(raw_dirs)} raw_dirs concluídos")

        logger.info("✅ Carga da Camada Bronze concluída com sucesso!")

    except Exception:
        logger.exception("❌ Erro durante a carga da Camada Bronze.")
        raise

def _load_raw_dir(
    raw_dir: str,
    dag_run_id: str,
    output_dir: str,
    memory_limit: Optional[str],
    duckdb_threads: int,
    ingestion_time: datetime,
) -> None:
    """
    Executa a ingestão de um único raw_dir.

    Cada worker cria sua própria conexão DuckDB.
    """

    con = None

    try:
        logger.info(f"Iniciando processamento: {raw_dir} (DuckDB threads={duckdb_threads})")

        con = duckdb.connect()
        # Reduz consumo de memória durante leitura/escrita.
        con.execute("SET preserve_insertion_order = false;")

        if memory_limit:
            logger.info(f"[{raw_dir}] memory_limit={memory_limit}")
            con.execute(f"SET memory_limit = '{memory_limit}';")

        # Threads internas desta conexão.
        con.execute(f"SET threads = {duckdb_threads};")

        con.execute(
            COPY_QUERY,
            {
                "input_dir": raw_dir,
                "output_dir": output_dir,
                "dag_run_id": dag_run_id,
                "ingestion_time": ingestion_time,
            },
        )

        logger.info(f"Concluído com sucesso: {raw_dir}")

    except Exception:
        logger.exception(f"Erro processando raw_dir: {raw_dir}")
        raise

    finally:
        if con is not None:
            con.close()

def _validate_memory_limit(memory_limit: Optional[str]) -> None:
    if memory_limit is None:
        return

    match = re.fullmatch(
        r"\s*(\d+(?:\.\d+)?|\.\d+)\s*(B|KB|MB|GB|TB)\s*",
        memory_limit,
        flags=re.IGNORECASE,
    )
    if match is None or float(match.group(1)) <= 0:
        raise ValueError(
            "memory_limit deve ser uma quantidade positiva em B, KB, MB, GB ou TB."
        )

def list_bronze_dirs(
    start_year: int,
    end_year: int,
    output_dir: str = OUTPUT_DIR,
) -> list[str]:
    """
    Gera os caminhos esperados das partições Bronze no intervalo solicitado.

    Parâmetros
    ----------
    start_year / end_year:
        Intervalo utilizado para localizar os diretórios Bronze.

    Retorna
    -------
    List[str]:
        Caminhos esperados das partições da Camada Bronze.
    """
    if start_year > end_year:
        raise ValueError("start_year deve ser menor ou igual a end_year.")

    return [
        os.path.join(output_dir, f"year={year}")
        for year in range(start_year, end_year + 1)
    ]