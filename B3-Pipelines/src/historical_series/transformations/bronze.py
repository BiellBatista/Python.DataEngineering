import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

import duckdb

from config import (
    BRONZE_COTAHIST_DIR,
    BRONZE_MAX_WORKERS,
    BRONZE_YEAR_PARTITION_TEMPLATE,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_MEMORY_LIMIT_PATTERN,
    DUCKDB_PRESERVE_INSERTION_ORDER,
    DUCKDB_THREADS,
    INGESTED_DATE_FORMAT,
    PARQUET_COMPRESSION,
    PARQUET_ROW_GROUP_SIZE,
    RAW_COTAHIST_DIR,
    RAW_FILENAME_YEAR_PATTERN,
    RAW_TEXT_DELIMITER,
    RAW_TEXT_QUOTE,
)
from historical_series.ingestion.raw import list_raw_dirs
from utils.helpers import ensure_directory_available

logger = logging.getLogger(__name__)

# Utilizado r"""...""" para preservar as barras invertidas do RegEx (\d)
COPY_QUERY = rf"""
    /*
    ===========================================================================
    INGESTÃO BRONZE - COTAHIST B3 (DuckDB / CSV -> Parquet Bruto)
    ===========================================================================
    Esta consulta lê os arquivos de texto bruto (TXT) originais da B3 e os 
    converte para o formato Parquet na camada Bronze, aplicando:

    1. Leitura Paralela de Texto Bruto (read_csv):
       - Lê arquivos linha por linha configurando o delimitador como quebra de 
         linha (`delim='\n'`) sem cabeçalho (`header=False`).
       - Desativa o tratamento de aspas (`quote=''`) para evitar falhas de 
         parsing com caracteres especiais no texto.
       - Lê a linha inteira como uma única coluna `VARCHAR` chamada `content`.
       - Habilita `filename=True` para capturar o nome do arquivo de origem e 
         `parallel=True` para acelerar a leitura multithread pelo DuckDB.

    2. Enriquecimento de Metadados e Particionamento:
       - _ingested_at: Registra o carimbo de data/hora exato da execução.
       - _source_file: Armazena o caminho/nome do arquivo original lido.
       - _dag_run_id: Identifica a execução do pipeline de orquestração.
       - year: Extrai dinamicamente o ano de 4 dígitos do nome do arquivo 
         (ex: 'COTAHIST_A2024.TXT') usando RegEx e `TRY_CAST` seguro contra falhas.
       - _ingested_date: Formata a data/hora da ingestão no padrão 
         'YYYY-MM-DD_HH-MM-SS' para versionamento das cargas.

    3. Escrita Otimizada na Camada Bronze (COPY TO):
       - Grava os dados brutos no formato `PARQUET`.
       - Organiza o particionamento em nível duplo: `PARTITION_BY (year, _ingested_date)`.
       - Aplica compressão `ZSTD` e define o `ROW_GROUP_SIZE` para 122.880 linhas.
    ===========================================================================
    */
    COPY (
        SELECT
            content,
            $ingestion_time AS _ingested_at,
            filename AS _source_file,
            $dag_run_id AS _dag_run_id,
            -- TRY_CAST evita quebra se algum arquivo não tiver o ano de 4 dígitos no nome
            TRY_CAST(regexp_extract(filename, '{RAW_FILENAME_YEAR_PATTERN}', 1) AS INTEGER) AS year,
            strftime($ingestion_time, '{INGESTED_DATE_FORMAT}') AS _ingested_date
        FROM read_csv(
            $input_dir,
            header=False,
            delim='{RAW_TEXT_DELIMITER}',
            quote='{RAW_TEXT_QUOTE}',
            columns={{'content': 'VARCHAR'}},
            filename=True,
            parallel=True
        )
    ) TO $output_dir (
        FORMAT PARQUET,
        PARTITION_BY (year, _ingested_date),
        COMPRESSION '{PARQUET_COMPRESSION}',
        ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE},
        OVERWRITE_OR_IGNORE
    );
"""

def execute_bronze_load(
    dag_run_id: str,
    start_year: int,
    end_year: int,
    memory_limit: Optional[str] = DUCKDB_MEMORY_LIMIT,
    threads: Optional[int] = DUCKDB_THREADS,
    max_workers: Optional[int] = BRONZE_MAX_WORKERS,
    input_dir: str = RAW_COTAHIST_DIR,
    output_dir: str = BRONZE_COTAHIST_DIR,
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
        worker_limit = max_workers if max_workers is not None else BRONZE_MAX_WORKERS
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
        # Desativa a ordenação estrita para permitir streaming direto de blocos para o disco pelas threads paralelas, otimizando o consumo de RAM.
        # Ajusta as configurações internas do motor DuckDB para otimizar o uso de memória RAM e aumentar o throughput em operações de leitura e escrita paralelas.
        #Define se o DuckDB deve manter a ordem exata de inserção dos registros lidos. Quando definido como False (recomendado para grandes cargas e pipelines ETL/ELT),
        # desativa a sincronização de reordenação entre threads de gravação, reduzindo drasticamente o consumo de memória RAM e eliminando gargalos de processamento paralelo.
        con.execute(
            f"SET preserve_insertion_order = "
            f"{str(DUCKDB_PRESERVE_INSERTION_ORDER).lower()};"
        )

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
        DUCKDB_MEMORY_LIMIT_PATTERN,
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
    output_dir: str = BRONZE_COTAHIST_DIR,
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
        os.path.join(
            output_dir,
            BRONZE_YEAR_PARTITION_TEMPLATE.format(year=year),
        )
        for year in range(start_year, end_year + 1)
    ]