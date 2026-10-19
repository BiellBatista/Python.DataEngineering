"""Deduplicação e escrita do dataset COTAHIST na camada Silver."""

import logging
import os
import re
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Optional

import duckdb

from common.exceptions import BronzeError, SilverError
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
    PARQUET_COMPRESSION,
    PARQUET_PER_THREAD_OUTPUT,
    PARQUET_ROW_GROUP_SIZE,
    SILVER_COTAHIST_DIR,
)
from historical_series.transformations.bronze import (
    find_most_recent_bronze_parquets,
    list_bronze_dirs,
)
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
       - Grava os dados em formato `PARQUET` particionado por ano.
       - Escreve um arquivo por partição e substitui o dataset apenas após a
         conclusão bem-sucedida da nova carga.
       - Aplica compressão `ZSTD` e define o `ROW_GROUP_SIZE` para 122.880 linhas,
         otimizando o desempenho de consultas e o data skipping.
    ===========================================================================
    */
    COPY (
        WITH parsed AS (
            SELECT
                year,
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
            year,
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
        PARTITION_BY (year),
        COMPRESSION '{PARQUET_COMPRESSION}',
        ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE},
        PER_THREAD_OUTPUT {str(PARQUET_PER_THREAD_OUTPUT).lower()},
        OVERWRITE
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
    """Consolida os Parquets Bronze e substitui as partições anuais processadas.

    Processa cada ano isoladamente: lê a ingestão Bronze mais recente, aplica
    transformação e deduplicação, grava em staging e promove somente a
    partição ``year=AAAA`` daquele ano. Outras partições Silver são preservadas.
    Se um ano falhar, os anos já promovidos permanecem atualizados e os demais
    ainda não processados não são alterados.

    Args:
        dag_run_id: Identificador da execução do pipeline, armazenado nos dados.
        start_year: Primeiro ano a processar.
        end_year: Último ano a processar, inclusive.
        memory_limit: Limite de memória DuckDB, ou ``None`` para o padrão.
        threads: Número de threads DuckDB; ``None`` usa as CPUs disponíveis.
        input_dir: Diretório raiz de entrada da camada Bronze.
        output_dir: Diretório raiz de saída da camada Silver.

    Raises:
        ValueError: Se os anos, threads ou limite de memória forem inválidos.
        BronzeError: Se não houver diretórios ou arquivos Parquet Bronze.
        SilverError: Se o caminho de saída existir e não for diretório.
        Exception: Propaga falhas de leitura ou transformação. A partição Silver
            anterior do ano corrente é preservada se a nova escrita falhar.
    """
    if start_year > end_year:
        raise ValueError("start_year deve ser menor ou igual a end_year.")
    if threads is not None and threads <= 0:
        raise ValueError("threads deve ser maior que zero.")

    _validate_memory_limit(memory_limit)

    try:
        output_path = Path(output_dir)
        ensure_directory_available(str(output_path.parent))
        if output_path.exists() and not output_path.is_dir():
            raise SilverError(
                f"O destino Silver existe e não é um diretório: {output_path}"
            )
        ensure_directory_available(str(output_path))

        logger.info("Iniciando carga da Camada Silver...")

        bronze_dirs = list_bronze_dirs(start_year, end_year, input_dir)
        missing_dirs = check_directories_exist(bronze_dirs)
        
        if missing_dirs:
            raise BronzeError(
                f"Diretórios Bronze inexistentes: {', '.join(missing_dirs)}"
            )

        total_threads = threads if threads is not None else (os.cpu_count() or 4)
        for bronze_dir in bronze_dirs:
            recent_parquets = find_most_recent_bronze_parquets(bronze_dir)

            if not recent_parquets:
                raise BronzeError(
                    f"Nenhum arquivo Parquet encontrado em: {bronze_dir}"
                )

            logger.info(
                "Processando %s arquivo(s) Bronze da partição mais recente em %s",
                len(recent_parquets),
                bronze_dir,
            )
            staging_dir = tempfile.mkdtemp(
                prefix=f".{output_path.name}.staging-",
                dir=output_path,
            )
            try:
                con = duckdb.connect(":memory:")
                try:
                    con.execute(
                        f"SET preserve_insertion_order = "
                        f"{str(DUCKDB_PRESERVE_INSERTION_ORDER).lower()};"
                    )
                    con.execute(f"SET threads = {total_threads};")

                    if memory_limit:
                        con.execute(f"SET memory_limit = '{memory_limit}';")

                    logger.info(
                        "Threads DuckDB configuradas: %s; ano=%s",
                        total_threads,
                        Path(bronze_dir).name,
                    )
                    con.execute(
                        COPY_QUERY,
                        {
                            "input_files": recent_parquets,
                            "output_dir": staging_dir,
                            "dag_run_id": dag_run_id,
                        },
                    )
                finally:
                    con.close()

                year_partition = Path(bronze_dir).name
                staged_partition = Path(staging_dir) / year_partition
                if not staged_partition.is_dir():
                    raise SilverError(
                        "A transformação Silver não gerou a partição esperada: "
                        f"{staged_partition}"
                    )

                _promote_output_directory(
                    staged_partition,
                    output_path / year_partition,
                )
                logger.info(
                    "Partição Silver %s atualizada com sucesso.",
                    year_partition,
                )
            finally:
                if os.path.exists(staging_dir):
                    shutil.rmtree(staging_dir)

        logger.info("✅ Carga da Camada Silver concluída com sucesso!")

    except Exception:
        logger.exception("❌ Erro durante a carga da Camada Silver.")
        raise


def _promote_output_directory(staging_dir: Path, output_dir: Path) -> None:
    """Promove a saída temporária e restaura a anterior se a promoção falhar.

    Args:
        staging_dir: Diretório temporário contendo o novo dataset completo.
        output_dir: Diretório final do dataset Silver.

    Raises:
        OSError: Se a substituição do diretório falhar. Quando possível, a
            versão anterior é restaurada antes de propagar o erro.
    """
    backup_dir = output_dir.with_name(
        f".{output_dir.name}.backup-{uuid.uuid4().hex}"
    )
    had_existing_output = output_dir.exists()

    try:
        if had_existing_output:
            os.replace(output_dir, backup_dir)

        os.replace(staging_dir, output_dir)
    except Exception:
        if had_existing_output and backup_dir.exists() and not output_dir.exists():
            os.replace(backup_dir, output_dir)
        raise

    if had_existing_output:
        shutil.rmtree(backup_dir)

def _validate_memory_limit(memory_limit: Optional[str]) -> None:
    """Valida se o limite de memória tem unidade aceita e valor positivo.

    Args:
        memory_limit: Limite, como ``"4GB"``, ou ``None``.

    Raises:
        ValueError: Se a string não representar um tamanho positivo válido.
    """
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
