import logging
import os
import re
from pathlib import Path
from typing import Optional

import duckdb

from config import (
    B3_CLOSE_PRICE_START,
    B3_DECIMAL_SCALE,
    B3_HIGH_PRICE_START,
    B3_LOW_PRICE_START,
    B3_MARKET_TYPE_LENGTH,
    B3_MARKET_TYPE_START,
    B3_OPEN_PRICE_START,
    B3_PRICE_LENGTH,
    B3_RECORD_TYPE_LENGTH,
    B3_RECORD_TYPE_CODE,
    B3_RECORD_TYPE_START,
    B3_TICKER_LENGTH,
    B3_TICKER_START,
    B3_TOTAL_VOLUME_LENGTH,
    B3_TOTAL_VOLUME_START,
    B3_TRADE_DATE_FORMAT,
    B3_TRADE_DATE_LENGTH,
    B3_TRADE_DATE_START,
    BRONZE_COTAHIST_DIR,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_MEMORY_LIMIT_PATTERN,
    DUCKDB_PRESERVE_INSERTION_ORDER,
    DUCKDB_THREADS,
    INGESTED_DATE_PARTITION_NAME,
    PARQUET_COMPRESSION,
    PARQUET_ROW_GROUP_SIZE,
    SILVER_COTAHIST_DIR,
)
from historical_series.transformations.bronze import list_bronze_dirs
from utils.helpers import check_directories_exist, ensure_directory_available

logger = logging.getLogger(__name__)

COPY_QUERY = f"""
    /*
    ===========================================================================
    TRANSFORMAÇÃO SILVER - COTAHIST B3 (DuckDB / Parquet)
    ===========================================================================
    Esta consulta executa a consolidação e limpeza dos dados brutos da camada 
    Bronze, aplicando as seguintes etapas:

    1. Leitura Dinâmica (read_parquet):
       - Lê os arquivos Parquet da Bronze com suporte a unificação automática 
         de esquemas (`union_by_name = true`) e hive partitioning.

    2. Filtro de Tipo de Registro (WHERE):
       - Restringe o processamento apenas aos registros de lote padrão 
         (`SUBSTR(content, 1, 2) = '01'`), descartando cabeçalhos e trailers.

    3. Parsing Posicional e Tipagem (CTE `parsed`):
       - Fatia a string bruta da coluna `content` conforme o layout oficial da B3:
         • data_pregao: Converte a string 'YYYYMMDD' para o tipo `DATE`.
         • ticker: Código do ativo tratado com `TRIM()`.
         • tipo_mercado: Código BDI do mercado.
         • Preços e Volumes: Conversão para `DOUBLE` e divisão por 100.0 
           para ajustar as casas decimais monetárias.

    4. Deduplicação e Idempotência (QUALIFY):
       - Utiliza uma Window Function (`row_number`) particionando por 
         `ticker`, `data_pregao` e `tipo_mercado`, ordenada de forma decrescente 
         pelo momento da ingestão (`_ingested_at`) e arquivo fonte.
       - Garante que reprocessamentos (idempotência) mantenham sempre 
         apenas a versão mais recente do registro.

    5. Escrita Otimizada na Camada Silver (COPY TO):
       - Grava os dados em formato `PARQUET` particionado por `data_pregao`.
       - Aplica compressão `ZSTD` e define o `ROW_GROUP_SIZE` para 122.880 linhas,
         otimizando o desempenho de consultas e o data skipping.
    ===========================================================================
    */
    COPY (
        WITH parsed AS (
            SELECT
                strptime(SUBSTR(content, {B3_TRADE_DATE_START}, {B3_TRADE_DATE_LENGTH}), '{B3_TRADE_DATE_FORMAT}')::DATE AS data_pregao,
                TRIM(SUBSTR(content, {B3_TICKER_START}, {B3_TICKER_LENGTH})) AS ticker,
                SUBSTR(content, {B3_MARKET_TYPE_START}, {B3_MARKET_TYPE_LENGTH}) AS tipo_mercado,
                CAST(SUBSTR(content, {B3_OPEN_PRICE_START}, {B3_PRICE_LENGTH}) AS DOUBLE) / {B3_DECIMAL_SCALE} AS preco_abertura,
                CAST(SUBSTR(content, {B3_HIGH_PRICE_START}, {B3_PRICE_LENGTH}) AS DOUBLE) / {B3_DECIMAL_SCALE} AS preco_maximo,
                CAST(SUBSTR(content, {B3_LOW_PRICE_START}, {B3_PRICE_LENGTH}) AS DOUBLE) / {B3_DECIMAL_SCALE} AS preco_minimo,
                CAST(SUBSTR(content, {B3_CLOSE_PRICE_START}, {B3_PRICE_LENGTH}) AS DOUBLE) / {B3_DECIMAL_SCALE} AS preco_fechamento,
                CAST(SUBSTR(content, {B3_TOTAL_VOLUME_START}, {B3_TOTAL_VOLUME_LENGTH}) AS DOUBLE) / {B3_DECIMAL_SCALE} AS volume_total,
                _ingested_at,
                _source_file,
                $dag_run_id AS _dag_run_id
            FROM read_parquet($input_files, union_by_name = true, hive_partitioning = true)
            WHERE SUBSTR(content, {B3_RECORD_TYPE_START}, {B3_RECORD_TYPE_LENGTH}) = '{B3_RECORD_TYPE_CODE}'
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
        COMPRESSION '{PARQUET_COMPRESSION}',
        ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE},
        OVERWRITE_OR_IGNORE
    );
"""

def execute_silver_load(
    dag_run_id: str,
    start_year: int,
    end_year: int,
    memory_limit: Optional[str] = DUCKDB_MEMORY_LIMIT,
    threads: Optional[int] = DUCKDB_THREADS,
    input_dir: str = BRONZE_COTAHIST_DIR,
    output_dir: str = SILVER_COTAHIST_DIR,
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
            con.execute(
                f"SET preserve_insertion_order = "
                f"{str(DUCKDB_PRESERVE_INSERTION_ORDER).lower()};"
            )
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
    ingest_dirs = [
        d
        for d in year_path.glob(f"{INGESTED_DATE_PARTITION_NAME}=*")
        if d.is_dir()
    ]

    if not ingest_dirs:
        return sorted(str(path) for path in year_path.rglob("*.parquet"))

    latest_dir = max(ingest_dirs, key=lambda d: d.name)
    return sorted(str(path) for path in latest_dir.rglob("*.parquet"))

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
