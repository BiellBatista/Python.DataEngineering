import duckdb
import logging
import os
import re
from pathlib import Path
from typing import Optional
from transformations.historical_series.bronze import list_bronze_dirs
from utils.helpers import check_directories_exist, ensure_directory_available

logger = logging.getLogger(__name__)

OUTPUT_DIR = "/mnt/d/b3_datalake/silver/cotacoes_historicas"
BRONZE_DIR = "/mnt/d/b3_datalake/bronze/cotahist"
COPY_QUERY = """
    COPY (
        WITH parsed AS (
            SELECT
                strptime(SUBSTR(content, 3, 8), '%Y%m%d')::DATE AS data_pregao,
                TRIM(SUBSTR(content, 13, 12)) AS ticker,
                SUBSTR(content, 25, 3) AS tipo_mercado,
                CAST(SUBSTR(content, 57, 13) AS DOUBLE) / 100.0 AS preco_abertura,
                CAST(SUBSTR(content, 70, 13) AS DOUBLE) / 100.0 AS preco_maximo,
                CAST(SUBSTR(content, 83, 13) AS DOUBLE) / 100.0 AS preco_minimo,
                CAST(SUBSTR(content, 109, 13) AS DOUBLE) / 100.0 AS preco_fechamento,
                CAST(SUBSTR(content, 171, 18) AS DOUBLE) / 100.0 AS volume_total,
                _ingested_at,
                _source_file,
                $dag_run_id AS _dag_run_id
            FROM read_parquet($input_files, union_by_name = true, hive_partitioning = true)
            WHERE SUBSTR(content, 1, 2) = '01'
        )
        SELECT
            data_pregao,
            ticker,
            tipo_mercado,
            preco_abertura,
            preco_maximo,
            preco_minimo,
            preco_fechamento,
            volume_total,
            _ingested_at,
            _source_file,
            _dag_run_id
        FROM parsed
        QUALIFY row_number() OVER (
            PARTITION BY ticker, data_pregao, tipo_mercado
            ORDER BY _ingested_at DESC, _source_file DESC
        ) = 1
    ) TO $output_dir (
        FORMAT PARQUET,
        PARTITION_BY (data_pregao),
        COMPRESSION 'ZSTD',
        ROW_GROUP_SIZE 122880,
        OVERWRITE_OR_IGNORE
    );
"""

def execute_silver_load(
    dag_run_id: str,
    start_year: int,
    end_year: int,
    memory_limit: Optional[str] = "4GB",
    threads: Optional[int] = None,
    input_dir: str = BRONZE_DIR,
    output_dir: str = OUTPUT_DIR,
) -> None:
    """
    Executa a carga da camada Silver processando a partição Bronze mais recente de cada ano.
    Processa todos os arquivos Parquet da partição Bronze mais recente de cada ano.

    input_dir / output_dir:
        Diretórios base das camadas Bronze e Silver.
    """
    if start_year > end_year:
        raise ValueError("start_year deve ser menor ou igual a end_year.")
    if threads is not None and threads <= 0:
        raise ValueError("threads deve ser maior que zero.")

    _validate_memory_limit(memory_limit)

    try:
        ensure_directory_available(output_dir)

        logger.info("Iniciando carga da Camada Silver...")

        bronze_dirs = list_bronze_dirs(start_year, end_year, input_dir)
        missing_dirs = check_directories_exist(bronze_dirs)
        
        if missing_dirs:
            raise FileNotFoundError(
                f"Diretórios Bronze inexistentes: {', '.join(missing_dirs)}"
            )

        total_threads = threads if threads is not None else (os.cpu_count() or 4)
        con = duckdb.connect(":memory:")

        try:
            con.execute("SET preserve_insertion_order = false;")
            con.execute(f"SET threads = {total_threads};")

            if memory_limit:
                con.execute(f"SET memory_limit = '{memory_limit}';")

            logger.info(f"Threads DuckDB configuradas: {total_threads}")

            for bronze_dir in bronze_dirs:
                recent_parquets = _find_most_recent_parquets(bronze_dir)

                if not recent_parquets:
                    raise FileNotFoundError(
                        f"Nenhum arquivo Parquet encontrado em: {bronze_dir}"
                    )

                logger.info(
                    "Processando %s arquivo(s) Bronze da partição mais recente em %s",
                    len(recent_parquets),
                    bronze_dir,
                )
                con.execute(
                    COPY_QUERY,
                    {
                        "input_files": recent_parquets,
                        "output_dir": output_dir,
                        "dag_run_id": dag_run_id,
                    },
                )
        finally:
            con.close()

        logger.info("✅ Carga da Camada Silver concluída com sucesso!")

    except Exception:
        logger.exception("❌ Erro durante a carga da Camada Silver.")
        raise

def _find_most_recent_parquets(bronze_year_dir: str) -> list[str]:
    """
    Retorna todos os Parquets da partição _ingested_date mais recente.
    """
    year_path = Path(bronze_year_dir)
    ingest_dirs = [d for d in year_path.glob("_ingested_date=*") if d.is_dir()]

    if not ingest_dirs:
        return sorted(str(path) for path in year_path.rglob("*.parquet"))

    latest_dir = max(ingest_dirs, key=lambda d: d.name)
    return sorted(str(path) for path in latest_dir.rglob("*.parquet"))

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
