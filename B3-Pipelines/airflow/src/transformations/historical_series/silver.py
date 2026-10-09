import logging
import os
import sys
from pathlib import Path
from typing import Optional
import duckdb

SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bronze import list_bronze_dirs
from utils.helpers import check_directories_exist, ensure_directory_available

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ingestao_b3_silver")

OUTPUT_DIR = "/mnt/d/b3_datalake/silver/cotacoes_historicas"
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
            FROM read_parquet($input_file, union_by_name = true, hive_partitioning = true)
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
            PARTITION BY ticker, data_pregao 
            ORDER BY _ingested_at DESC
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
    threads: Optional[int] = None
) -> None:
    """
    Executa a carga da camada Silver processando a partição Bronze mais recente de cada ano.
    Utiliza multithreading nativo do DuckDB para gravação paralela via PER_THREAD_OUTPUT.
    """
    try:
        if not ensure_directory_available(OUTPUT_DIR):
            logger.error(f"Diretório de saída indisponível: {OUTPUT_DIR}")
            return

        logger.info("Iniciando carga da Camada Silver...")
        bronze_dirs = list_bronze_dirs(start_year, end_year)

        if not bronze_dirs:
            logger.warning("Nenhum diretório Bronze encontrado.")
            return

        missing_dirs = check_directories_exist(bronze_dirs)

        if missing_dirs:
            logger.error(f"Diretórios Bronze inexistentes: {missing_dirs}")
            return

        total_threads = threads or (os.cpu_count() or 4)
        
        # Conexão centralizada em memória aproveitando todas as threads do DuckDB
        con = duckdb.connect(":memory:")
        con.execute("SET preserve_insertion_order = false;")
        con.execute(f"SET threads = {total_threads};")

        if memory_limit:
            con.execute(f"SET memory_limit = '{memory_limit}';")

        logger.info(f"Threads DuckDB configuradas: {total_threads}")

        for bronze_dir in bronze_dirs:
            recent_parquet = _find_most_recent_parquet(bronze_dir)

            if not recent_parquet:
                logger.warning(f"Nenhum arquivo Parquet encontrado em: {bronze_dir}")
                continue

            logger.info(f"Processando arquivo Bronze: {recent_parquet}")

            con.execute(
                COPY_QUERY,
                {
                    "input_file": recent_parquet,
                    "output_dir": OUTPUT_DIR,
                    "dag_run_id": dag_run_id,
                },
            )

        con.close()
        logger.info("✅ Carga da Camada Silver concluída com sucesso!")

    except Exception:
        logger.exception("❌ Erro durante a carga da Camada Silver.")
        raise

def _find_most_recent_parquet(bronze_year_dir: str) -> Optional[str]:
    """
    Encontra o arquivo Parquet na partição _ingested_date=YYYY-MM-DD_HH-MM-SS mais recente.
    """
    year_path = Path(bronze_year_dir)
    ingest_dirs = [d for d in year_path.glob("_ingested_date=*") if d.is_dir()]

    if not ingest_dirs:
        parquets = list(year_path.rglob("*.parquet"))
        return str(max(parquets, key=lambda x: x.stat().st_mtime)) if parquets else None

    # Ordenação lexicográfica natural pelo nome da partição ISO (_ingested_date=...)
    latest_dir = max(ingest_dirs, key=lambda d: d.name)
    parquets = list(latest_dir.glob("*.parquet"))

    return str(parquets[0]) if parquets else None

execute_silver_load("teste-123", 2024, 2025, memory_limit="8GB", threads=8)
