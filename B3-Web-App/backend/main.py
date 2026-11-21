"""API web do Data Explorer, conectada ao DuckDBServer via Quack."""

import base64
import logging
import math
import os
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from time import perf_counter
from typing import Iterator

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from opentelemetry.trace import Span, Status, StatusCode
from pydantic import BaseModel, ConfigDict, Field

from backend.observability import (
    logger,
    observe_http_requests,
    observability_lifespan,
    record_frontend_event,
    record_query_metrics,
    tracer,
)

DATA_LAKE_ROOT = Path(os.getenv("B3_DATA_LAKE_ROOT", "/data-lake"))
DUCKDB_HOST = os.getenv("DUCKDB_HOST", "duckdb-server")
DUCKDB_PORT = int(os.getenv("DUCKDB_PORT", "9494"))
QUACK_TOKEN = os.getenv("QUACK_TOKEN", "")
MAX_QUERY_ROWS = int(os.getenv("MAX_QUERY_ROWS", "1000"))
LAYERS = ("bronze", "silver")
STATIC_DIR = Path(__file__).resolve().parent / "static"

_MAX_QUERY_ROWS_LIMIT = 100_000

app = FastAPI(
    title="B3 Data Explorer",
    version="0.1.0",
    lifespan=observability_lifespan,
)
app.middleware("http")(observe_http_requests)


class QueryRequest(BaseModel):
    sql: str = Field(min_length=1, max_length=100_000)


class FrontendLogEvent(str, Enum):
    APPLICATION_STARTED = "aplicacao_iniciada"
    CATALOG_LOADED = "catalogo_carregado"
    CATALOG_LOAD_FAILED = "falha_ao_carregar_catalogo"
    QUERY_STARTED = "consulta_iniciada"
    QUERY_COMPLETED = "consulta_concluida"
    QUERY_FAILED = "falha_na_consulta"
    INTERFACE_ERROR = "erro_na_interface"


class FrontendLogRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event: FrontendLogEvent
    duration_ms: float | None = Field(default=None, ge=0, le=300_000)
    catalog_tables: int | None = Field(default=None, ge=0, le=10_000)
    result_rows: int | None = Field(default=None, ge=0, le=100_000)


class QueryValidationError(ValueError):
    """SQL ausente, inválido ou diferente de uma consulta SELECT única."""


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _connect_duckdb() -> duckdb.DuckDBPyConnection:
    if not QUACK_TOKEN:
        raise RuntimeError("QUACK_TOKEN não foi configurado.")
    if not 1 <= DUCKDB_PORT <= 65535:
        raise RuntimeError("DUCKDB_PORT deve estar entre 1 e 65535.")

    with tracer.start_as_current_span("duckdb.connect") as span:
        span.set_attribute("db.system.name", "duckdb")
        span.set_attribute("server.address", DUCKDB_HOST)
        span.set_attribute("server.port", DUCKDB_PORT)
        connection = duckdb.connect(":memory:")
        try:
            connection.execute("INSTALL quack")
            connection.execute("LOAD quack")
            uri = f"quack:{DUCKDB_HOST}:{DUCKDB_PORT}"
            connection.execute(
                f"ATTACH '{_escape_sql_string(uri)}' AS remote "
                f"(TYPE quack, TOKEN '{_escape_sql_string(QUACK_TOKEN)}', "
                "DISABLE_SSL true)"
            )
            connection.execute("CONNECT remote")
        except Exception:
            span.set_status(
                Status(
                    StatusCode.ERROR,
                    "Falha ao conectar ao DuckDBServer.",
                )
            )
            connection.close()
            raise

    return connection


def _discover_tables() -> list[dict[str, str]]:
    if not DATA_LAKE_ROOT.is_dir():
        raise OSError(f"Data Lake não encontrado: {DATA_LAKE_ROOT}")

    tables: list[dict[str, str]] = []
    for layer in LAYERS:
        layer_path = DATA_LAKE_ROOT / layer
        if not layer_path.is_dir():
            continue

        dataset_paths: list[tuple[str, Path]] = []
        for child in sorted(layer_path.iterdir()):
            if (
                child.is_dir()
                and "=" not in child.name
                and next(child.rglob("*.parquet"), None) is not None
            ):
                dataset_paths.append((child.name, child))

        if next(layer_path.glob("*.parquet"), None) is not None:
            dataset_paths.append(("data", layer_path))
        elif not dataset_paths and next(layer_path.rglob("*.parquet"), None) is not None:
            dataset_paths.append(("data", layer_path))

        seen_names: set[str] = set()
        for table_name, table_path in dataset_paths:
            normalized_name = table_name.casefold()
            if normalized_name in seen_names:
                raise ValueError(
                    f"Nome de dataset duplicado na camada {layer}: {table_name}"
                )
            seen_names.add(normalized_name)

            parquet_pattern = _escape_sql_string(
                str(table_path / "**" / "*.parquet")
            )
            tables.append(
                {
                    "layer": layer,
                    "name": table_name,
                    "qualified_name": (
                        f"{_quote_identifier(layer)}."
                        f"{_quote_identifier(table_name)}"
                    ),
                    "parquet_pattern": parquet_pattern,
                }
            )

    return tables


def _register_tables(
    connection: duckdb.DuckDBPyConnection,
    tables: list[dict[str, str]],
) -> None:
    for layer in LAYERS:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(layer)}")

    for table in tables:
        connection.execute(
            f"CREATE OR REPLACE VIEW "
            f"{_quote_identifier(table['layer'])}."
            f"{_quote_identifier(table['name'])} AS "
            "SELECT * FROM read_parquet("
            f"'{table['parquet_pattern']}', hive_partitioning = true)"
        )


@contextmanager
def _ready_connection() -> Iterator[tuple[duckdb.DuckDBPyConnection, list[dict[str, str]]]]:
    try:
        connection = _connect_duckdb()
    except (duckdb.Error, RuntimeError) as error:
        raise HTTPException(
            status_code=503,
            detail=f"Não foi possível conectar ao DuckDBServer: {error}",
        ) from error

    try:
        tables = _discover_tables()
        with tracer.start_as_current_span("duckdb.catalog.register") as span:
            span.set_attribute("db.system.name", "duckdb")
            span.set_attribute("b3.catalog.dataset_count", len(tables))
            try:
                _register_tables(connection, tables)
            except (duckdb.Error, ValueError):
                span.set_status(
                    Status(
                        StatusCode.ERROR,
                        "Falha ao registrar o catálogo do Data Lake.",
                    )
                )
                raise
        yield connection, tables
    except OSError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    except (duckdb.Error, ValueError) as error:
        raise HTTPException(
            status_code=503,
            detail=f"Não foi possível preparar o catálogo do Data Lake: {error}",
        ) from error
    finally:
        connection.close()


def _validate_query(sql: str) -> None:
    try:
        statements = duckdb.extract_statements(sql)
    except duckdb.Error as error:
        raise QueryValidationError(str(error)) from error

    if len(statements) != 1:
        raise QueryValidationError("Informe exatamente uma instrução SQL.")
    if statements[0].type != duckdb.StatementType.SELECT:
        raise QueryValidationError("Somente consultas SELECT são permitidas.")


def _json_value(value: object) -> object:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return str(value)


@app.get("/api/health")
def health() -> dict[str, str]:
    try:
        with _ready_connection() as (connection, _):
            connection.execute("SELECT 1")
    except HTTPException:
        raise
    except duckdb.Error as error:
        raise HTTPException(
            status_code=503,
            detail=f"DuckDBServer indisponível: {error}",
        ) from error
    return {"status": "ok"}


@app.get("/api/catalog")
def catalog() -> dict[str, list[dict[str, object]]]:
    with _ready_connection() as (_, tables):
        return {
            "layers": [
                {
                    "name": layer,
                    "tables": [
                        {
                            "name": table["name"],
                            "qualified_name": table["qualified_name"],
                        }
                        for table in tables
                        if table["layer"] == layer
                    ],
                }
                for layer in LAYERS
            ]
        }


@app.post("/api/logs", status_code=202)
def frontend_log(request: FrontendLogRequest) -> dict[str, str]:
    warning_events = {
        FrontendLogEvent.CATALOG_LOAD_FAILED,
        FrontendLogEvent.QUERY_FAILED,
    }
    error_events = {FrontendLogEvent.INTERFACE_ERROR}
    record_frontend_event(request.event.value, request.duration_ms)
    level = (
        logging.ERROR
        if request.event in error_events
        else logging.WARNING
        if request.event in warning_events
        else logging.INFO
    )
    logger.log(
        level,
        "Evento de observabilidade recebido do frontend.",
        extra={
            "frontend_event": request.event.value,
            "duration_ms": request.duration_ms,
            "catalog_tables": request.catalog_tables,
            "result_rows": request.result_rows,
        },
    )
    return {"status": "recebido"}


@app.post("/api/query")
def query(request: QueryRequest) -> dict[str, object]:
    started_at = perf_counter()
    outcome = "falha"
    try:
        try:
            _validate_query(request.sql)
        except QueryValidationError as error:
            outcome = "rejeitada"
            logger.warning(
                "Consulta rejeitada pela política somente leitura.",
                extra={"outcome": outcome, "error_type": type(error).__name__},
            )
            raise HTTPException(status_code=400, detail=str(error)) from error

        if not 1 <= MAX_QUERY_ROWS <= _MAX_QUERY_ROWS_LIMIT:
            raise HTTPException(
                status_code=500,
                detail=(
                    f"MAX_QUERY_ROWS deve estar entre 1 e "
                    f"{_MAX_QUERY_ROWS_LIMIT}."
                ),
            )

        with _ready_connection() as (connection, _):
            with tracer.start_as_current_span("duckdb.query") as span:
                span.set_attribute("db.system.name", "duckdb")
                span.set_attribute("server.address", DUCKDB_HOST)
                span.set_attribute("server.port", DUCKDB_PORT)
                span.set_attribute("db.operation.name", "SELECT")
                try:
                    connection.execute(request.sql)
                    columns = [
                        {
                            "name": description[0],
                            "type": str(description[1]),
                        }
                        for description in connection.description or []
                    ]
                    fetched_rows = connection.fetchmany(MAX_QUERY_ROWS + 1)
                    truncated = len(fetched_rows) > MAX_QUERY_ROWS
                    rows = fetched_rows[:MAX_QUERY_ROWS]
                    span.set_attribute("db.response.returned_rows", len(rows))
                    span.set_attribute("b3.query.truncated", truncated)
                except duckdb.Error as error:
                    span.set_status(
                        Status(
                            StatusCode.ERROR,
                            "DuckDB rejeitou a consulta SQL.",
                        )
                    )
                    logger.warning(
                        "DuckDB rejeitou a consulta SQL.",
                        extra={
                            "outcome": "falha",
                            "error_type": type(error).__name__,
                        },
                    )
                    raise HTTPException(
                        status_code=400,
                        detail=str(error),
                    ) from error

        outcome = "sucesso"
        return {
            "columns": columns,
            "rows": [[_json_value(value) for value in row] for row in rows],
            "row_count": len(rows),
            "truncated": truncated,
            "max_rows": MAX_QUERY_ROWS,
        }
    finally:
        record_query_metrics(outcome, perf_counter() - started_at)


app.mount(
    "/",
    StaticFiles(directory=STATIC_DIR, html=True, check_dir=False),
    name="frontend",
)
