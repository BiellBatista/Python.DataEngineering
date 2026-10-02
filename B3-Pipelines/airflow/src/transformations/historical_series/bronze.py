import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import duckdb

from ingestion.historical_series.raw import list_raw_dirs
from utils.helpers import validate_output_dir

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ingestao_b3_bronze")

OUTPUT_DIR = "D:/b3_datalake/bronze/cotahist"
COPY_QUERY = """
    COPY (
        SELECT
            content,
            now() AS _ingested_at,
            filename AS _source_file,
            $dag_run_id AS _dag_run_id,
            -- 1ª Chave de Partição: Ano extraído do nome do arquivo (ex: 'COTAPHIST_A2024.TXT' -> 2024)
            CAST(regexp_extract(filename, 'A({4})', 1) AS INTEGER) AS year,
            -- 2ª Chave de Partição: Data e Hora de ingestão tratadas para compatibilidade de sistema de arquivos (sem ':')
            strftime(now(), '%Y-%m-%d_%H-%M-%S') AS _ingested_date
        FROM read_csv(
            $input_dir,
            header=False,
            delimiter='\n',
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

def execute_bronze_load(dag_run_id, start_year, end_year, memory_limit: Optional[str] = None, threads: Optional[int] = None, max_workers: Optional[int] = None) -> None:
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
    """

    try:
        output_dir_is_valid = validate_output_dir(OUTPUT_DIR)

        if not output_dir_is_valid:
            logger.error(f"Diretório de saída inválido: {OUTPUT_DIR}")
            return

        logger.info("Iniciando carga da Camada Bronze...")

        raw_dirs = list_raw_dirs(start_year, end_year)

        if not raw_dirs:
            logger.warning("Nenhum diretório RAW encontrado. Nada para processar.")
            return

        total_threads = threads or (os.cpu_count() or 1)

        # Evita criar workers demais.
        #
        # Exemplo:
        # CPU = 16
        # raw_dirs = 10
        # -> no máximo 4 workers
        workers = min(max_workers or 4, len(raw_dirs), total_threads)

        # Divide o orçamento total de threads
        # entre os workers.
        threads_per_worker = max(1, total_threads // workers,)

        logger.info(f"RAW dirs encontrados: {len(raw_dirs)}")
        logger.info(f"Workers paralelos: {workers}")
        logger.info(f"Threads DuckDB por worker: {threads_per_worker}")
        logger.info(f"Threads totais máximas: {workers * threads_per_worker}")
        logger.info(f"Destino: {OUTPUT_DIR}")

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="bronze-worker",) as executor:
            futures = {
                executor.submit(
                    _load_raw_dir,
                    raw_dir,
                    dag_run_id,
                    OUTPUT_DIR,
                    memory_limit,
                    threads_per_worker,
                ): raw_dir
                for raw_dir in raw_dirs
            }

            completed = 0

            for future in as_completed(futures):
                raw_dir = futures[future]

                # Se algum worker falhar, a exceção é
                # propagada para esta função.
                future.result()

                completed += 1

                logger.info(f"Progresso: {completed}/{len(raw_dirs)} raw_dirs concluídos")

        logger.info("✅ Carga da Camada Bronze concluída com sucesso!")

    except Exception:
        logger.exception("❌ Erro durante a carga da Camada Bronze.")
        raise

def _load_raw_dir(raw_dir: str, dag_run_id, output_dir: str, memory_limit: Optional[str], duckdb_threads: int) -> None:
    """
    Executa a ingestão de um único raw_dir.

    Cada worker cria sua própria conexão DuckDB.
    """
    con = None

    try:
        logger.info(
            f"Iniciando processamento: {raw_dir}"
            f"(DuckDB threads={duckdb_threads})"
        )

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
            },
        )

        logger.info(f"Concluído com sucesso: {raw_dir}")

    except Exception:
        logger.exception(f"Erro processando raw_dir: {raw_dir}")
        raise

    finally:
        if con is not None:
            con.close()

execute_bronze_load("teste-123", 2024, 2025, memory_limit="8GB", threads=8, max_workers=4)