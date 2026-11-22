"""Geração do produto Gold de candles COTAHIST particionados por ticker.

Este módulo implementa somente o produto ``gold/candles``. Cada produto Gold
deve ter sua própria transformação, evitando concentrar contratos e regras de
datasets diferentes em uma carga Gold genérica.
"""

import logging
import os
import re
import shutil
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextvars import copy_context
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

from common.exceptions import GoldError
from common.log_context import log_parameters
from config import (
    DUCKDB_HOST,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_MEMORY_LIMIT_PATTERN,
    DUCKDB_THREADS,
    GOLD_CANDLES_DIR,
    GOLD_MAX_WORKERS,
    PARQUET_COMPRESSION,
    PARQUET_PER_THREAD_OUTPUT,
    PARQUET_ROW_GROUP_SIZE,
    SILVER_COTAHIST_DIR,
)
from utils.duckdb_connection import connect_duckdb, execute_query
from utils.helpers import ensure_directory_available

logger = logging.getLogger(__name__)

# Filtros de negócio aplicados às linhas Silver antes da formação dos candles.
# Compartilhado pela consulta de candles e pela consulta de tickers válidos.
_SILVER_ROW_FILTERS = """
            WHERE ticker IS NOT NULL
              AND TRIM(ticker) <> ''
              AND data_pregao IS NOT NULL
              AND TRIM(tipo_mercado) = '010'
              AND preco_abertura > 0
              AND preco_minimo > 0
              AND preco_minimo <= preco_abertura
              AND preco_minimo <= preco_fechamento
              AND preco_maximo >= preco_abertura
              AND preco_maximo >= preco_fechamento
"""

TICKERS_QUERY = f"""
    /*
    ===========================================================================
    GOLD CANDLES - RESOLUÇÃO DE TICKERS VÁLIDOS (DuckDB)
    ===========================================================================
    Lista os tickers distintos da Silver que produzem ao menos um candle
    diário válido. O Python aplica sobre essa lista os prefixos de ticker
    informados e distribui o resultado entre os workers paralelos da carga
    Gold.
    ===========================================================================
    */
    SELECT DISTINCT TRIM(ticker) AS ticker
    FROM read_parquet(
        $input_files,
        union_by_name = true,
        hive_partitioning = true
    )
    {_SILVER_ROW_FILTERS}
    ORDER BY ticker;
"""

COPY_QUERY = f"""
    /*
    ===========================================================================
    GOLD CANDLES - COTAHIST B3 (DuckDB / Parquet)
    ===========================================================================
    Produz o dataset analítico de candles OHLC a partir da Silver. A consulta
    executa as seguintes etapas:

    1. Leitura do dataset Silver (read_parquet):
       - Lê a lista de arquivos Silver encontrada pelo coordenador Python.
       - `union_by_name = true` tolera diferenças de esquema entre arquivos;
         `hive_partitioning = true` reconhece as partições existentes.

    2. Seleção e validação dos registros (WHERE):
       - Exige ticker e data preenchidos e tipo de mercado `010`, que é o
         formato de três caracteres gravado pela transformação Silver.
       - Mantém preços positivos e respeita as relações OHLC:
         mínima <= abertura/último negócio e máxima >= abertura/último negócio.
       - `$tickers` é sempre uma lista explícita: o Python resolve os
         prefixos informados sobre os tickers válidos da Silver e distribui
         o resultado entre workers paralelos, cada um executando este COPY
         para um subconjunto de tickers.

    3. Projeção e validação dos candles diários (CTE `daily`):
       - Renomeia os campos para o contrato de candles (`time`, `open`,
         `high`, `low` e `close`) e mantém valores monetários como
         `DECIMAL(18, 2)`.
       - `close` vem de `preco_fechamento`, que na Silver corresponde ao
         campo COTAHIST PREULT (último negócio), não ao fechamento oficial.

    4. Agregação mensal (CTE `monthly`):
       - Agrupa os candles diários por ticker e mês (`timeframe = '1m'`):
         abertura do primeiro pregão, máxima, mínima, fechamento do último
         pregão e soma dos volumes. O `time` é o primeiro dia do mês.

    5. União dos timeframes e ordenação:
       - Combina candles diários (`timeframe = '1d'`, padrão do COTAHIST) e
         mensais (`timeframe = '1m'`) na mesma saída, ordenados por ticker,
         timeframe e data.
       - `_ingested_at` registra o horário da carga Gold e `_dag_run_id` o
         identificador da execução do pipeline.

    6. Escrita do produto Gold (COPY TO):
       - Cada worker grava Parquet em seu próprio staging e particiona por
         ticker, gerando caminhos como `ticker=VALE3/`.
       - Usa compressão ZSTD, row groups de 122.880 linhas e a configuração
         compartilhada de saída por thread.
       - O Python promove apenas as partições geradas depois que todos os
         COPYs terminam com sucesso, preservando tickers não selecionados.
    ===========================================================================
    */
    COPY (
        WITH daily AS (
            SELECT
                TRIM(ticker) AS ticker,
                CAST(data_pregao AS DATE) AS time,
                TRIM(tipo_mercado) AS tipo_mercado,
                CAST(preco_abertura AS DECIMAL(18, 2)) AS open,
                CAST(preco_maximo AS DECIMAL(18, 2)) AS high,
                CAST(preco_minimo AS DECIMAL(18, 2)) AS low,
                CAST(preco_fechamento AS DECIMAL(18, 2)) AS close,
                CAST(volume_total AS DECIMAL(18, 2)) AS volume_total
            FROM read_parquet(
                $input_files,
                union_by_name = true,
                hive_partitioning = true
            )
            {_SILVER_ROW_FILTERS}
              AND (
                  $tickers IS NULL
                  OR list_contains($tickers, TRIM(ticker))
              )
        ),
        monthly AS (
            SELECT
                ticker,
                date_trunc('month', time)::DATE AS time,
                MIN(tipo_mercado) AS tipo_mercado,
                -- OHLC mensal: abertura do 1º pregão, máxima, mínima,
                -- fechamento do último pregão e soma dos volumes.
                -- arg_min/arg_max resolvem "primeiro/último pregão" do mês.
                arg_min(open, time) AS open,
                max(high) AS high,
                min(low) AS low,
                arg_max(close, time) AS close,
                sum(volume_total) AS volume_total
            FROM daily
            GROUP BY ticker, date_trunc('month', time)::DATE
        )
        SELECT
            ticker,
            '1d' AS timeframe,
            time,
            tipo_mercado,
            open,
            high,
            low,
            close,
            volume_total,
            $ingestion_time AS _ingested_at,
            $dag_run_id AS _dag_run_id
        FROM daily
        UNION ALL
        SELECT
            ticker,
            '1m' AS timeframe,
            time,
            tipo_mercado,
            open,
            high,
            low,
            close,
            volume_total,
            $ingestion_time AS _ingested_at,
            $dag_run_id AS _dag_run_id
        FROM monthly
        ORDER BY ticker ASC, timeframe ASC, time ASC
    ) TO $output_dir (
        FORMAT PARQUET,
        PARTITION_BY (ticker),
        COMPRESSION '{PARQUET_COMPRESSION}',
        ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE},
        PER_THREAD_OUTPUT {str(PARQUET_PER_THREAD_OUTPUT).lower()},
        OVERWRITE_OR_IGNORE
    );
"""


@log_parameters
def execute_gold_candles_load(
    start_with: Optional[Sequence[str]] = None,
    dag_run_id: Optional[str] = None,
    memory_limit: Optional[str] = DUCKDB_MEMORY_LIMIT,
    threads: Optional[int] = DUCKDB_THREADS,
    max_workers: Optional[int] = GOLD_MAX_WORKERS,
    input_dir: str = SILVER_COTAHIST_DIR,
    output_dir: str = GOLD_CANDLES_DIR,
) -> None:
    """Gera o produto ``gold/candles`` e atualiza os tickers selecionados.

    Se nenhum seletor for informado, processa todos os tickers válidos da
    Silver. ``start_with`` aceita uma lista de prefixos e processa os tickers
    que começam com qualquer um deles. O resultado é particionado por ticker,
    por exemplo ``gold/candles/ticker=VALE3``, e cada partição contém os
    candles diários (``timeframe = '1d'``) e mensais (``timeframe = '1m'``)
    do ticker.

    A carga resolve os tickers válidos da Silver, distribui o resultado entre
    workers paralelos (cada um com conexão DuckDB exclusiva e staging
    próprio) e promove cada partição do ticker individualmente após a
    conclusão de todos os COPYs. Se uma escrita falhar, as partições finais
    não são alteradas; se a promoção de uma partição falhar, tenta restaurar
    a versão anterior. Partições já promovidas antes de outra partição falhar
    permanecem atualizadas.

    Args:
        start_with: Lista de prefixos; processa tickers que começam com
            qualquer um deles. ``None`` processa todos os tickers válidos.
        dag_run_id: Identificador da execução do pipeline, gravado na coluna
            ``_dag_run_id``. ``None`` grava ``NULL``. O horário de ``_ingested_at``
            é sempre o instante atual em UTC, como na camada Silver.
        memory_limit: Limite de memória do DuckDB, ou ``None`` para o padrão.
            No modo Quack, deve corresponder ao limite global do servidor.
        threads: Número de threads DuckDB. ``None`` usa as CPUs locais; no modo
            Quack, o valor deve corresponder à configuração global do servidor.
        max_workers: Número máximo de grupos de tickers processados em
            paralelo. ``None`` usa o padrão configurado.
        input_dir: Diretório raiz dos Parquets Silver.
        output_dir: Diretório raiz de saída do produto Gold de candles.

    Raises:
        ValueError: Se o seletor, threads, workers ou limite de memória forem
            inválidos.
        GoldError: Se a Silver não existir, não houver arquivos Parquet, o
            destino for inválido ou nenhum ticker gerar candles válidos.
        Exception: Propaga falhas de conexão, transformação ou escrita.
    """
    normalized_start = _normalize_selector_list(start_with, "start_with")
    if threads is not None and threads <= 0:
        raise ValueError("threads deve ser maior que zero.")
    if max_workers is not None and max_workers <= 0:
        raise ValueError("max_workers deve ser maior que zero.")
    _validate_memory_limit(memory_limit)

    try:
        silver_path = Path(input_dir)
        if not silver_path.is_dir():
            raise GoldError(
                f"O diretório Silver não existe ou não é um diretório: {silver_path}"
            )
        input_files = sorted(str(path) for path in silver_path.rglob("*.parquet"))
        if not input_files:
            raise GoldError(
                f"Nenhum arquivo Parquet Silver encontrado em: {silver_path}"
            )

        if normalized_start is not None:
            ticker_selection = f"start_with={normalized_start}"
        else:
            ticker_selection = "todos"

        output_path = Path(output_dir)
        ensure_directory_available(str(output_path.parent))
        if output_path.exists() and not output_path.is_dir():
            raise GoldError(
                f"O destino Gold existe e não é um diretório: {output_path}"
            )
        ensure_directory_available(str(output_path))

        total_threads = threads if threads is not None else (os.cpu_count() or 4)
        logger.info(
            "Iniciando carga Gold de candles: entrada=%s; arquivos_silver=%s; "
            "destino=%s; seleção=%s; threads=%s; memory_limit=%s",
            silver_path,
            len(input_files),
            output_path,
            ticker_selection,
            total_threads,
            memory_limit,
        )

        valid_tickers = _select_valid_tickers(
            input_files,
            memory_limit,
            total_threads,
        )
        selected_tickers = _apply_ticker_prefixes(
            valid_tickers,
            normalized_start,
        )
        if not selected_tickers:
            raise GoldError(
                "Nenhum ticker selecionado gerou candles válidos na camada Gold."
            )

        worker_limit = (
            max_workers if max_workers is not None else GOLD_MAX_WORKERS
        )
        workers = min(worker_limit, len(selected_tickers))
        if DUCKDB_HOST:
            threads_per_worker = total_threads
        else:
            workers = min(workers, total_threads)
            threads_per_worker = max(1, total_threads // workers)
        ticker_chunks = _chunk_tickers(selected_tickers, workers)

        logger.info("Tickers Gold a processar: %s", len(selected_tickers))
        logger.info("Workers paralelos: %s", workers)
        if DUCKDB_HOST:
            logger.info("Threads DuckDB globais no servidor: %s", total_threads)
        else:
            logger.info("Threads DuckDB por worker: %s", threads_per_worker)
        logger.info(
            "Threads totais máximas: %s", workers * threads_per_worker
        )

        staging_dir = tempfile.mkdtemp(
            prefix=f".{output_path.name}.staging-",
            dir=output_path,
        )
        ingestion_time = datetime.now(timezone.utc).replace(tzinfo=None)
        try:
            with ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="gold-worker",
            ) as executor:
                futures = {
                    executor.submit(
                        copy_context().run,
                        _load_ticker_chunk,
                        chunk,
                        input_files,
                        Path(staging_dir) / f"worker-{index}",
                        dag_run_id,
                        ingestion_time,
                        memory_limit,
                        threads_per_worker,
                    ): index
                    for index, chunk in enumerate(ticker_chunks)
                }
                completed = 0

                for future in as_completed(futures):
                    future.result()
                    completed += 1
                    logger.info(
                        "Progresso: %s/%s grupos de tickers Gold concluídos",
                        completed,
                        len(ticker_chunks),
                    )

            staged_partitions = sorted(
                Path(staging_dir).glob("worker-*/ticker=*")
            )
            if not staged_partitions:
                raise GoldError(
                    "Nenhum ticker selecionado gerou candles válidos na camada Gold."
                )

            for staged_partition in staged_partitions:
                _promote_ticker_partition(
                    staged_partition,
                    output_path / staged_partition.name,
                )
                logger.info(
                    "Partição Gold %s atualizada com sucesso.",
                    staged_partition.name,
                )
        finally:
            if os.path.exists(staging_dir):
                shutil.rmtree(staging_dir)

        logger.info("Carga Gold de candles concluída com sucesso.")
        logger.info(
            "Partições de candles Gold atualizadas: %s",
            len(staged_partitions),
        )
    except Exception:
        logger.exception("Erro durante a carga Gold de candles.")
        raise


def _select_valid_tickers(
    input_files: list[str],
    memory_limit: Optional[str],
    threads: int,
) -> list[str]:
    """Lista os tickers da Silver que produzem ao menos um candle válido.

    Args:
        input_files: Caminhos dos Parquets Silver de entrada.
        memory_limit: Limite de memória do DuckDB, ou ``None`` para o padrão.
        threads: Número de threads da conexão DuckDB desta consulta.

    Returns:
        Tickers distintos e ordenados que atendem aos filtros de negócio.
    """
    connection = connect_duckdb(
        memory_limit=memory_limit,
        threads=threads,
    )
    try:
        result = execute_query(
            connection,
            TICKERS_QUERY,
            {"input_files": input_files},
        )
        return [row[0] for row in result.fetchall()]
    finally:
        connection.close()


def _apply_ticker_prefixes(
    valid_tickers: Sequence[str],
    start_with: Optional[list[str]],
) -> list[str]:
    """Filtra os tickers válidos pelos prefixos informados.

    Args:
        valid_tickers: Tickers que produzem candles válidos, já ordenados.
        start_with: Prefixos permitidos, ou ``None`` para todos.

    Returns:
        Tickers selecionados, preservando a ordem de ``valid_tickers``.
    """
    if start_with is None:
        return list(valid_tickers)
    return [
        ticker
        for ticker in valid_tickers
        if any(ticker.startswith(prefix) for prefix in start_with)
    ]


def _chunk_tickers(tickers: list[str], workers: int) -> list[list[str]]:
    """Distribui tickers entre workers de forma balanceada (round-robin).

    Args:
        tickers: Tickers selecionados, já ordenados.
        workers: Quantidade de grupos de trabalho.

    Returns:
        Listas de tickers não vazias, uma por worker utilizado.
    """
    chunks: list[list[str]] = [[] for _ in range(workers)]
    for index, ticker in enumerate(tickers):
        chunks[index % workers].append(ticker)
    return [chunk for chunk in chunks if chunk]


def _load_ticker_chunk(
    tickers: list[str],
    input_files: list[str],
    output_dir: Path,
    dag_run_id: Optional[str],
    ingestion_time: datetime,
    memory_limit: Optional[str],
    duckdb_threads: int,
) -> None:
    """Gera os candles Gold de um grupo de tickers com conexão exclusiva.

    Args:
        tickers: Tickers exatos processados por este worker.
        input_files: Caminhos dos Parquets Silver de entrada.
        output_dir: Diretório de staging exclusivo do worker.
        dag_run_id: Identificador da execução, gravado em ``_dag_run_id``.
        ingestion_time: Horário UTC da carga, gravado em ``_ingested_at``.
        memory_limit: Limite da instância DuckDB, ou ``None`` para o padrão.
        duckdb_threads: Número de threads global no modo Quack ou permitido
            nesta conexão no modo local.

    Raises:
        Exception: Propaga erros de conexão ou execução da consulta DuckDB.
    """
    connection = connect_duckdb(
        memory_limit=memory_limit,
        threads=duckdb_threads,
    )
    try:
        execute_query(
            connection,
            COPY_QUERY,
            {
                "input_files": input_files,
                "output_dir": str(output_dir),
                "tickers": tickers,
                "dag_run_id": dag_run_id,
                "ingestion_time": ingestion_time,
            },
        )
    finally:
        connection.close()


def _normalize_selector_list(
    value: Optional[Sequence[str]],
    name: str,
) -> Optional[list[str]]:
    """Valida e normaliza a lista de prefixos de ticker.

    Args:
        value: Seletor informado como texto único ou sequência de textos;
            ``None`` quando não usado.
        name: Nome do argumento, usado na mensagem de validação.

    Returns:
        Seletores sem espaços externos e sem duplicatas; ``None`` quando
        nenhum seletor foi especificado.

    Raises:
        ValueError: Se algum item não for um texto não vazio.
    """
    if value is None:
        return None
    items = [value] if isinstance(value, str) else list(value)
    normalized = []
    for item in items:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{name} deve conter apenas textos não vazios.")
        normalized.append(item.strip())
    return list(dict.fromkeys(normalized)) or None


def _promote_ticker_partition(staging_dir: Path, output_dir: Path) -> None:
    """Promove atomicamente uma partição e restaura a versão anterior no erro.

    Args:
        staging_dir: Diretório ``ticker=...`` criado pelo COPY Gold.
        output_dir: Diretório final para a mesma partição de ticker.

    Raises:
        OSError: Se a substituição do diretório falhar. Quando existe uma
            partição anterior, tenta restaurá-la antes de propagar a falha.
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
    """Valida formato e valor positivo do limite de memória DuckDB.

    Args:
        memory_limit: Quantidade com unidade B, KB, MB, GB ou TB; ``None``
            mantém o padrão do DuckDB.

    Raises:
        ValueError: Se o limite informado não for positivo ou tiver unidade
            não suportada.
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
