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
from pathlib import Path
from typing import Optional, Sequence

from common.exceptions import GoldError
from common.log_context import log_parameters
from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_MEMORY_LIMIT_PATTERN,
    DUCKDB_THREADS,
    GOLD_CANDLES_DIR,
    PARQUET_COMPRESSION,
    PARQUET_PER_THREAD_OUTPUT,
    PARQUET_ROW_GROUP_SIZE,
    SILVER_COTAHIST_DIR,
)
from utils.duckdb_connection import connect_duckdb, execute_query
from utils.helpers import ensure_directory_available

logger = logging.getLogger(__name__)

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
       - Pode filtrar por uma lista de tickers exatos, por prefixo
         (`start_with`) ou por sufixo (`end_with`). Esses seletores são
         mutuamente exclusivos; sem seletor, considera todos os tickers.

    3. Projeção e ordenação:
       - Renomeia os campos para o contrato de candles (`time`, `open`,
         `high`, `low` e `close`) e mantém valores monetários como
         `DECIMAL(18, 2)`.
       - `close` vem de `preco_fechamento`, que na Silver corresponde ao
         campo COTAHIST PREULT (último negócio), não ao fechamento oficial.
       - Ordena por ticker e data antes da escrita.

    4. Escrita do produto Gold (COPY TO):
       - Grava Parquet em staging e particiona por ticker, gerando caminhos
         como `ticker=VALE3/`.
       - Usa compressão ZSTD, row groups de 122.880 linhas e a configuração
         compartilhada de saída por thread.
       - O Python promove apenas as partições geradas depois que o COPY
         termina com sucesso, preservando tickers não selecionados.
    ===========================================================================
    */
    COPY (
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
          AND (
              $tickers IS NULL
              OR list_contains($tickers, TRIM(ticker))
          )
          AND (
              $start_with IS NULL
              OR starts_with(TRIM(ticker), $start_with)
          )
          AND (
              $end_with IS NULL
              OR ends_with(TRIM(ticker), $end_with)
          )
        ORDER BY ticker ASC, time ASC
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
    tickers: Optional[Sequence[str]] = None,
    start_with: Optional[str] = None,
    end_with: Optional[str] = None,
    memory_limit: Optional[str] = DUCKDB_MEMORY_LIMIT,
    threads: Optional[int] = DUCKDB_THREADS,
    input_dir: str = SILVER_COTAHIST_DIR,
    output_dir: str = GOLD_CANDLES_DIR,
) -> None:
    """Gera o produto ``gold/candles`` e atualiza os tickers selecionados.

    Se nenhum seletor for informado, processa todos os tickers válidos da
    Silver. Se informado, somente um modo de seleção pode ser usado: lista
    explícita, prefixo ``start_with`` ou sufixo ``end_with``. O resultado é
    particionado por ticker, por exemplo ``gold/candles/ticker=VALE3``.

    A carga localiza os Parquets Silver sem materializá-los no processo
    Python, prepara o Parquet Gold em um diretório temporário e promove cada
    partição do ticker individualmente. Se uma escrita falhar, as partições
    finais não são alteradas; se a promoção de uma partição falhar, tenta
    restaurar a versão anterior. Partições já promovidas antes de outra
    partição falhar permanecem atualizadas.

    Args:
        tickers: Lista não vazia de códigos exatos. Espaços externos são
            removidos e códigos repetidos são ignorados.
        start_with: Processa tickers cujo código começa com este texto.
        end_with: Processa tickers cujo código termina com este texto.
        memory_limit: Limite de memória do DuckDB, ou ``None`` para o padrão.
            No modo Quack, deve corresponder ao limite global do servidor.
        threads: Número de threads DuckDB. ``None`` usa as CPUs locais; no modo
            Quack, o valor deve corresponder à configuração global do servidor.
        input_dir: Diretório raiz dos Parquets Silver.
        output_dir: Diretório raiz de saída do produto Gold de candles.

    Raises:
        ValueError: Se seletores, threads ou limite de memória forem inválidos.
        GoldError: Se a Silver não existir, não houver arquivos Parquet, o
            destino for inválido ou nenhum ticker gerar candles válidos.
        Exception: Propaga falhas de conexão, transformação ou escrita.
    """
    normalized_tickers = _normalize_tickers(tickers)
    normalized_start = _normalize_selector(start_with, "start_with")
    normalized_end = _normalize_selector(end_with, "end_with")
    active_selectors = sum(
        selector is not None
        for selector in (normalized_tickers, normalized_start, normalized_end)
    )
    if active_selectors > 1:
        raise ValueError(
            "Informe somente um seletor: tickers, start_with ou end_with."
        )
    if threads is not None and threads <= 0:
        raise ValueError("threads deve ser maior que zero.")
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

        if normalized_tickers is not None:
            ticker_selection = f"tickers={normalized_tickers}"
        elif normalized_start is not None:
            ticker_selection = f"start_with={normalized_start}"
        elif normalized_end is not None:
            ticker_selection = f"end_with={normalized_end}"
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

        staging_dir = tempfile.mkdtemp(
            prefix=f".{output_path.name}.staging-",
            dir=output_path,
        )
        try:
            connection = connect_duckdb(
                memory_limit=memory_limit,
                threads=total_threads,
            )
            try:
                execute_query(
                    connection,
                    COPY_QUERY,
                    {
                        "input_files": input_files,
                        "output_dir": staging_dir,
                        "tickers": normalized_tickers,
                        "start_with": normalized_start,
                        "end_with": normalized_end,
                    },
                )
            finally:
                connection.close()

            staged_partitions = sorted(Path(staging_dir).glob("ticker=*"))
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


def _normalize_tickers(tickers: Optional[Sequence[str]]) -> Optional[list[str]]:
    """Normaliza a seleção exata de tickers.

    Args:
        tickers: Códigos exatos, ``None`` ou uma lista vazia para não filtrar.

    Returns:
        Códigos sem espaços externos e sem duplicatas; ``None`` quando nenhum
        ticker foi especificado.

    Raises:
        ValueError: Se a entrada for uma string simples ou contiver um código
            que não seja texto não vazio.
    """
    if tickers is None:
        return None
    if isinstance(tickers, str):
        raise ValueError("tickers deve ser uma lista de códigos.")
    normalized = []
    for ticker in tickers:
        if not isinstance(ticker, str) or not ticker.strip():
            raise ValueError("Cada ticker deve ser um código não vazio.")
        normalized.append(ticker.strip())
    return list(dict.fromkeys(normalized)) or None


def _normalize_selector(value: Optional[str], name: str) -> Optional[str]:
    """Valida e normaliza um prefixo ou sufixo.

    Args:
        value: Seletor informado, ou ``None`` quando não usado.
        name: Nome do argumento, usado na mensagem de validação.

    Returns:
        Seletor sem espaços externos ou ``None``.

    Raises:
        ValueError: Se o seletor informado não for um texto não vazio.
    """
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} deve ser um texto não vazio.")
    return value.strip()


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
