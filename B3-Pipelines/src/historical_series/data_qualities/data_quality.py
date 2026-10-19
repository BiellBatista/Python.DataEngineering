"""Validações de Data Quality para as camadas RAW e Bronze."""

import logging
from pathlib import Path
from typing import Callable, Optional

import duckdb

from common.exceptions import BronzeError, DataQualityError
from config import (
    B3_CLOSE_PRICE_START,
    B3_DECIMAL_SCALE,
    B3_FRACTIONAL_MARKET_TYPE,
    B3_HIGH_PRICE_START,
    B3_LOW_PRICE_START,
    B3_MARKET_TYPE_LENGTH,
    B3_MARKET_TYPE_START,
    B3_OPEN_PRICE_START,
    B3_PRICE_LENGTH,
    B3_RECORD_LENGTH,
    B3_RECORD_TYPE_CODES,
    B3_RECORD_TYPE_CODE,
    B3_RECORD_TYPE_LENGTH,
    B3_RECORD_TYPE_START,
    B3_TICKER_LENGTH,
    B3_TICKER_START,
    B3_TRADE_DATE_FORMAT,
    B3_TRADE_DATE_LENGTH,
    B3_TRADE_DATE_START,
    BRONZE_COTAHIST_DIR,
    BRONZE_YEAR_PARTITION_TEMPLATE,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    RAW_COTAHIST_DIR,
)
from historical_series.ingestions.raw import list_raw_dirs
from historical_series.transformations.bronze import (
    find_most_recent_bronze_parquets,
)

logger = logging.getLogger(__name__)


def validate_raw_quality(
    start_year: int,
    end_year: int,
    input_dir: str = RAW_COTAHIST_DIR,
) -> None:
    """Valida existência, conteúdo e largura posicional dos arquivos RAW.

    Args:
        start_year: Primeiro ano do intervalo, inclusive.
        end_year: Último ano do intervalo, inclusive.
        input_dir: Diretório raiz dos arquivos RAW.

    Raises:
        RawError: Se algum arquivo esperado estiver ausente.
        DataQualityError: Se um arquivo estiver vazio, não tiver registros ou
            contiver linhas incompatíveis com o layout fixo da B3.
    """
    _run_validation(
        "RAW",
        lambda: _validate_raw_quality(start_year, end_year, input_dir),
    )


def _validate_raw_quality(
    start_year: int,
    end_year: int,
    input_dir: str,
) -> None:
    """Executa as verificações de conteúdo dos arquivos RAW."""
    raw_files = list_raw_dirs(start_year, end_year, input_dir)
    valid_record_types = {
        record_type.encode("ascii") for record_type in B3_RECORD_TYPE_CODES
    }

    logger.info(
        "Validando %s arquivo(s) RAW para o período %s-%s.",
        len(raw_files),
        start_year,
        end_year,
    )
    for raw_file in raw_files:
        path = Path(raw_file)
        if path.stat().st_size == 0:
            raise DataQualityError(f"Data Quality RAW: arquivo vazio: {raw_file}")

        records = 0
        with path.open("rb") as source:
            for line_number, line in enumerate(source, start=1):
                record = line.removesuffix(b"\n").removesuffix(b"\r")
                if not record:
                    raise DataQualityError(
                        f"Data Quality RAW: registro vazio em {raw_file}, "
                        f"linha {line_number}."
                    )
                if len(record) != B3_RECORD_LENGTH:
                    raise DataQualityError(
                        f"Data Quality RAW: largura inválida em {raw_file}, "
                        f"linha {line_number}; esperado {B3_RECORD_LENGTH} "
                        f"bytes, encontrado {len(record)}."
                    )
                if record[:B3_RECORD_TYPE_LENGTH] not in valid_record_types:
                    record_type = record[:B3_RECORD_TYPE_LENGTH].decode(
                        "ascii",
                        errors="replace",
                    )
                    raise DataQualityError(
                        f"Data Quality RAW: tipo de registro inválido "
                        f"'{record_type}' em {raw_file}, linha {line_number}; "
                        f"tipos aceitos: {', '.join(B3_RECORD_TYPE_CODES)}."
                    )
                records += 1

        if records == 0:
            raise DataQualityError(
                f"Data Quality RAW: nenhum registro encontrado em {raw_file}."
            )
        logger.info(
            "Validação RAW aprovada para %s: %s registro(s).",
            raw_file,
            records,
        )


def validate_bronze_quality(
    start_year: int,
    end_year: int,
    input_dir: str = BRONZE_COTAHIST_DIR,
    memory_limit: Optional[str] = DUCKDB_MEMORY_LIMIT,
    threads: Optional[int] = DUCKDB_THREADS,
) -> None:
    """Valida chaves e regras de preço nos registros de negociação da Bronze.

    Somente os Parquets da ingestão mais recente de cada ano são verificados,
    correspondendo exatamente ao conjunto consumido pela transformação Silver.
    O campo de preço chamado ``preco_fechamento`` no pipeline corresponde ao
    ``PREULT`` do layout da B3, isto é, o preço do último negócio do pregão.
    Nos Parquets analisados, os registros do mercado fracionário (tipo ``020``)
    podem ter os quatro campos de preço zerados mesmo havendo negócios. Esse
    padrão é tratado como preço não informado; outras inconsistências continuam
    sendo validadas.

    Args:
        start_year: Primeiro ano do intervalo, inclusive.
        end_year: Último ano do intervalo, inclusive.
        input_dir: Diretório raiz da camada Bronze.
        memory_limit: Limite de memória DuckDB ou ``None`` para o padrão.
        threads: Número de threads DuckDB ou ``None`` para o padrão.

    Raises:
        ValueError: Se anos, limite de memória ou threads forem inválidos.
        DataQualityError: Se qualquer regra de qualidade falhar.
        BronzeError: Se faltar uma partição ou Parquet Bronze do intervalo.
        duckdb.Error: Se DuckDB não conseguir ler ou validar os arquivos.
    """
    _run_validation(
        "Bronze",
        lambda: _validate_bronze_quality(
            start_year,
            end_year,
            input_dir,
            memory_limit,
            threads,
        ),
    )


def _validate_bronze_quality(
    start_year: int,
    end_year: int,
    input_dir: str,
    memory_limit: Optional[str],
    threads: Optional[int],
) -> None:
    """Executa as verificações de regra de negócio sobre os Parquets Bronze."""
    if start_year > end_year:
        raise ValueError("start_year deve ser menor ou igual a end_year.")
    if threads is not None and threads <= 0:
        raise ValueError("threads deve ser maior que zero.")

    input_files: list[str] = []
    for year in range(start_year, end_year + 1):
        year_dir = Path(input_dir) / BRONZE_YEAR_PARTITION_TEMPLATE.format(
            year=year
        )
        if not year_dir.is_dir():
            raise BronzeError(
                f"Data Quality Bronze: partição anual inexistente: {year_dir}"
            )

        year_files = find_most_recent_bronze_parquets(str(year_dir))
        if not year_files:
            raise BronzeError(
                f"Data Quality Bronze: nenhum Parquet encontrado em {year_dir}"
            )
        input_files.extend(year_files)

    logger.info(
        "Validando %s arquivo(s) Parquet Bronze do período %s-%s.",
        len(input_files),
        start_year,
        end_year,
    )
    connection_config: dict[str, str] = {}
    if memory_limit is not None:
        connection_config["memory_limit"] = memory_limit
    if threads is not None:
        connection_config["threads"] = str(threads)

    con = duckdb.connect(config=connection_config)
    try:
        counts = con.execute(
            f"""
            WITH parsed AS (
                SELECT
                    SUBSTR(
                        content, {B3_MARKET_TYPE_START}, {B3_MARKET_TYPE_LENGTH}
                    ) AS tipo_mercado,
                    try_strptime(
                        SUBSTR(content, {B3_TRADE_DATE_START}, {B3_TRADE_DATE_LENGTH}),
                        '{B3_TRADE_DATE_FORMAT}'
                    ) AS data_pregao,
                    NULLIF(
                        TRIM(SUBSTR(content, {B3_TICKER_START}, {B3_TICKER_LENGTH})),
                        ''
                    ) AS ticker,
                    TRY_CAST(NULLIF(TRIM(SUBSTR(
                        content, {B3_OPEN_PRICE_START}, {B3_PRICE_LENGTH}
                    )), '') AS DOUBLE) / {B3_DECIMAL_SCALE} AS preco_abertura,
                    TRY_CAST(NULLIF(TRIM(SUBSTR(
                        content, {B3_HIGH_PRICE_START}, {B3_PRICE_LENGTH}
                    )), '') AS DOUBLE) / {B3_DECIMAL_SCALE} AS preco_maximo,
                    TRY_CAST(NULLIF(TRIM(SUBSTR(
                        content, {B3_LOW_PRICE_START}, {B3_PRICE_LENGTH}
                    )), '') AS DOUBLE) / {B3_DECIMAL_SCALE} AS preco_minimo,
                    TRY_CAST(NULLIF(TRIM(SUBSTR(
                        content, {B3_CLOSE_PRICE_START}, {B3_PRICE_LENGTH}
                    )), '') AS DOUBLE) / {B3_DECIMAL_SCALE} AS preco_fechamento
                FROM read_parquet(
                    $input_files,
                    union_by_name = true,
                    hive_partitioning = true
                )
                WHERE SUBSTR(
                    content, {B3_RECORD_TYPE_START}, {B3_RECORD_TYPE_LENGTH}
                ) = '{B3_RECORD_TYPE_CODE}'
            )
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE data_pregao IS NULL OR ticker IS NULL
                ) AS invalid_keys,
                COUNT(*) FILTER (
                    WHERE NOT COALESCE(
                        tipo_mercado = '{B3_FRACTIONAL_MARKET_TYPE}'
                        AND preco_abertura = 0
                        AND preco_maximo = 0
                        AND preco_minimo = 0
                        AND preco_fechamento = 0,
                        FALSE
                    )
                    AND (
                        preco_abertura IS NULL OR preco_abertura <= 0
                        OR preco_maximo IS NULL OR preco_maximo <= 0
                        OR preco_minimo IS NULL OR preco_minimo <= 0
                        OR preco_fechamento IS NULL OR preco_fechamento <= 0
                    )
                ) AS invalid_prices,
                COUNT(*) FILTER (
                    WHERE tipo_mercado = '{B3_FRACTIONAL_MARKET_TYPE}'
                      AND preco_abertura = 0
                      AND preco_maximo = 0
                      AND preco_minimo = 0
                      AND preco_fechamento = 0
                ) AS unpriced_fractional_records,
                COUNT(*) FILTER (
                    WHERE preco_maximo IS NOT NULL
                      AND preco_minimo IS NOT NULL
                      AND preco_maximo < preco_minimo
                ) AS invalid_price_ranges
            FROM parsed
            """,
            {"input_files": input_files},
        ).fetchone()
    finally:
        con.close()

    if counts is None:
        raise RuntimeError("Data Quality Bronze: DuckDB não retornou resultados.")

    (
        total,
        invalid_keys,
        invalid_prices,
        unpriced_fractional_records,
        invalid_price_ranges,
    ) = counts
    logger.info(
        "Resultado DQ Bronze: registros=%s, chaves_invalidas=%s, "
        "precos_invalidos=%s, registros_fracionarios_sem_preco=%s, "
        "faixas_invalidas=%s.",
        total,
        invalid_keys,
        invalid_prices,
        unpriced_fractional_records,
        invalid_price_ranges,
    )
    failures = []
    if total == 0:
        failures.append("nenhum registro de negociação encontrado")
    if invalid_keys:
        failures.append(f"{invalid_keys} registro(s) com chaves nulas ou inválidas")
    if invalid_prices:
        failures.append(f"{invalid_prices} registro(s) com preços nulos ou <= 0")
    if invalid_price_ranges:
        failures.append(
            f"{invalid_price_ranges} registro(s) com preço máximo menor que o mínimo"
        )

    if failures:
        raise DataQualityError(
            "Data Quality Bronze falhou: " + "; ".join(failures) + "."
        )


def _run_validation(layer: str, validation: Callable[[], None]) -> None:
    """Registra início, sucesso ou falha de uma validação de qualidade.

    Args:
        layer: Camada submetida à validação.
        validation: Função que executa as verificações para a camada.

    Raises:
        Exception: Propaga qualquer falha da validação após registrá-la.
    """
    logger.info("Iniciando Data Quality da camada %s.", layer)
    try:
        validation()
    except Exception:
        logger.exception("Data Quality da camada %s falhou.", layer)
        raise
    logger.info("Data Quality da camada %s aprovada.", layer)
