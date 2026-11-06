"""Cria conexões DuckDB locais ou roteadas para o servidor Quack.

O serviço Quack compartilha uma única instância DuckDB entre todas as tarefas.
Por isso, as configurações globais de memória e threads são aplicadas uma vez
na inicialização do servidor; clientes remotos não devem alterá-las por tarefa.
"""

import math
import re
from datetime import date, datetime
from typing import Mapping

import duckdb

from config import (
    DUCKDB_HOST,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_PRESERVE_INSERTION_ORDER,
    DUCKDB_PORT,
    DUCKDB_THREADS,
    QUACK_TOKEN,
)

_PARAMETER_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")


def connect_duckdb(
    memory_limit: str | None = None,
    threads: int | None = None,
    preserve_insertion_order: bool = DUCKDB_PRESERVE_INSERTION_ORDER,
) -> duckdb.DuckDBPyConnection:
    """Conecta ao servidor Quack ou usa uma instância DuckDB local.

    Quando ``DUCKDB_HOST`` está configurado, o servidor Quack controla a
    configuração global da instância. Os limites solicitados devem coincidir
    com os valores compartilhados do ambiente; para alterá-los, reinicie o
    serviço DuckDB com novos valores em ``.env``. Fora do Compose, limites são
    aplicados diretamente à conexão embutida local.

    Args:
        memory_limit: Limite DuckDB solicitado, como ``"8GB"``.
        threads: Número de threads DuckDB solicitado.
        preserve_insertion_order: Se a ordem de inserção deve ser preservada.

    Returns:
        Conexão local ou conexão cliente roteada para o servidor Quack.

    Raises:
        ValueError: Se a configuração remota divergir do servidor ou faltar
            ``QUACK_TOKEN``.
        duckdb.Error: Se a conexão ou configuração DuckDB falhar.
    """
    connection = duckdb.connect(":memory:")

    try:
        if DUCKDB_HOST:
            if not QUACK_TOKEN:
                raise ValueError(
                    "QUACK_TOKEN é obrigatório quando DUCKDB_HOST está configurado."
                )
            if memory_limit is not None and memory_limit != DUCKDB_MEMORY_LIMIT:
                raise ValueError(
                    "memory_limit é global no servidor Quack e deve corresponder "
                    "a DUCKDB_MEMORY_LIMIT configurado no ambiente."
                )
            if threads is not None and threads != DUCKDB_THREADS:
                raise ValueError(
                    "threads é global no servidor Quack e deve corresponder "
                    "a DUCKDB_THREADS configurado no ambiente."
                )
            if preserve_insertion_order != DUCKDB_PRESERVE_INSERTION_ORDER:
                raise ValueError(
                    "preserve_insertion_order é global no servidor Quack e não "
                    "pode ser alterado por tarefa."
                )

            connection.execute("INSTALL quack")
            connection.execute("LOAD quack")
            uri = f"quack:{DUCKDB_HOST}:{DUCKDB_PORT}"
            connection.execute(
                f"ATTACH '{_escape_sql_string(uri)}' AS remote "
                f"(TYPE quack, TOKEN '{_escape_sql_string(QUACK_TOKEN)}', "
                "DISABLE_SSL true)"
            )
            connection.execute("CONNECT remote")
        else:
            connection.execute(
                "SET preserve_insertion_order = "
                f"{str(preserve_insertion_order).lower()}"
            )
            if memory_limit is not None:
                _validate_memory_limit(memory_limit)
                connection.execute(f"SET memory_limit = '{memory_limit}'")
            if threads is not None:
                if threads <= 0:
                    raise ValueError("threads deve ser maior que zero.")
                connection.execute(f"SET threads = {threads}")
    except Exception:
        connection.close()
        raise

    return connection


def execute_query(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parameters: Mapping[str, object] | None = None,
) -> duckdb.DuckDBPyConnection:
    """Executa SQL pelo Python API, renderizando parâmetros para uso com Quack.

    O protocolo remoto Quack não aceita parâmetros nomeados passados ao método
    ``execute`` do cliente Python. Substituí-los por literais SQL escapados
    mantém a consulta no DuckDB e evita trazer dados para o processo Python.

    Args:
        connection: Conexão DuckDB local ou remota já inicializada.
        query: Instrução SQL com marcadores nomeados ``$nome``.
        parameters: Valores tipados que substituem os marcadores.

    Returns:
        A mesma conexão após executar a instrução.

    Raises:
        ValueError: Se faltar marcador ou algum valor fornecido não for usado.
            Também ocorre quando a consulta contém string ou comentário sem
            fechamento.
        TypeError: Se um parâmetro tiver um tipo que não pode ser literal SQL.
        ValueError: Se um float informado não for finito.
        duckdb.Error: Se a consulta falhar no motor DuckDB.
    """
    parameter_values = parameters or {}
    rendered_query, used_parameters = _render_parameters(query, parameter_values)
    unused_parameters = parameter_values.keys() - used_parameters
    if unused_parameters:
        raise ValueError(
            "Parâmetro(s) SQL não utilizado(s): "
            + ", ".join(sorted(unused_parameters))
        )
    return connection.execute(rendered_query)


def _render_parameters(
    query: str,
    parameters: Mapping[str, object],
) -> tuple[str, set[str]]:
    """Substitui marcadores fora de literais e comentários SQL.

    Args:
        query: Instrução SQL original.
        parameters: Valores que substituem marcadores nomeados.

    Returns:
        Consulta renderizada e nomes dos parâmetros efetivamente usados.

    Raises:
        ValueError: Se a consulta referenciar um parâmetro ausente.
    """
    chunks: list[str] = []
    used_parameters: set[str] = set()
    index = 0
    block_comment_depth = 0
    quote: str | None = None
    line_comment = False

    while index < len(query):
        current = query[index]
        following = query[index + 1] if index + 1 < len(query) else ""

        if line_comment:
            chunks.append(current)
            index += 1
            if current == "\n":
                line_comment = False
            continue

        if block_comment_depth:
            if current == "/" and following == "*":
                chunks.append("/*")
                block_comment_depth += 1
                index += 2
            elif current == "*" and following == "/":
                chunks.append("*/")
                block_comment_depth -= 1
                index += 2
            else:
                chunks.append(current)
                index += 1
            continue

        if quote is not None:
            chunks.append(current)
            index += 1
            if current == quote:
                if following == quote:
                    chunks.append(following)
                    index += 1
                else:
                    quote = None
            continue

        if current == "-" and following == "-":
            chunks.append("--")
            line_comment = True
            index += 2
        elif current == "/" and following == "*":
            chunks.append("/*")
            block_comment_depth = 1
            index += 2
        elif current in ("'", '"'):
            chunks.append(current)
            quote = current
            index += 1
        elif current == "$" and (
            match := _PARAMETER_PATTERN.match(query, index)
        ):
            name = match.group(1)
            if name not in parameters:
                raise ValueError(f"Parâmetro SQL não fornecido: {name}")
            used_parameters.add(name)
            chunks.append(_sql_literal(parameters[name]))
            index = match.end()
        else:
            chunks.append(current)
            index += 1

    if quote is not None or block_comment_depth:
        raise ValueError("A consulta SQL termina com uma string ou comentário aberto.")

    return "".join(chunks), used_parameters


def _sql_literal(value: object) -> str:
    """Formata um valor Python suportado como literal SQL com escaping.

    Args:
        value: Valor escalar, data/hora, sequência ou ``None``.

    Returns:
        Representação SQL literal adequada ao tipo.

    Raises:
        TypeError: Se o tipo não tiver conversão explícita e segura.
        ValueError: Se o valor float não for finito.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("Parâmetros float devem ser valores finitos.")
        return repr(value)
    if isinstance(value, datetime):
        literal_type = "TIMESTAMPTZ" if value.utcoffset() is not None else "TIMESTAMP"
        return f"{literal_type} '{_escape_sql_string(value.isoformat())}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, str):
        return f"'{_escape_sql_string(value)}'"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_sql_literal(item) for item in value) + "]"
    raise TypeError(f"Tipo não suportado como parâmetro SQL: {type(value).__name__}")


def _escape_sql_string(value: str) -> str:
    """Escapa aspas simples para incluir texto em um literal SQL.

    Args:
        value: Texto que será serializado como literal SQL.

    Returns:
        Texto com aspas simples duplicadas.
    """
    return value.replace("'", "''")


def _validate_memory_limit(memory_limit: str) -> None:
    """Valida o formato do limite de memória antes de interpolá-lo no SQL.

    Args:
        memory_limit: Limite como ``"2GB"`` ou ``"512MB"``.

    Raises:
        ValueError: Se o formato ou a quantidade forem inválidos.
    """
    match = re.fullmatch(
        r"\s*(\d+(?:\.\d+)?|\.\d+)\s*(B|KB|MB|GB|TB)\s*",
        memory_limit,
        re.IGNORECASE,
    )
    if match is None or float(match.group(1)) <= 0:
        raise ValueError(
            "memory_limit deve ser uma quantidade positiva em B, KB, MB, GB ou TB."
        )
