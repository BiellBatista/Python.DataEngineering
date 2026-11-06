"""Executa uma instância DuckDB persistente com o protocolo remoto Quack.

O servidor processa consultas e I/O sobre o data lake compartilhado pelos
containers. Seus limites de memória e threads são globais para a instância.
"""

import logging
import os
import re
from threading import Event

import duckdb

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_PRESERVE_INSERTION_ORDER,
    DUCKDB_THREADS,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_MEMORY_LIMIT_PATTERN = re.compile(
    r"\s*(\d+(?:\.\d+)?|\.\d+)\s*(B|KB|MB|GB|TB)\s*",
    re.IGNORECASE,
)


def main() -> None:
    """Inicia o servidor Quack e mantém sua conexão ativa.

    Lê endpoint, token e limites globais das variáveis de ambiente. A conexão
    fica aberta durante a vida do processo para atender as sessões dos clientes.

    Raises:
        KeyError: Se ``QUACK_TOKEN`` não estiver configurado.
        ValueError: Se o limite de memória não tiver valor positivo válido.
        duckdb.Error: Se DuckDB não conseguir configurar ou iniciar Quack.
    """
    host = os.getenv("DUCKDB_HOST", "0.0.0.0")
    port = int(os.getenv("DUCKDB_PORT", "9494"))
    token = os.environ["QUACK_TOKEN"]
    memory_limit_match = _MEMORY_LIMIT_PATTERN.fullmatch(DUCKDB_MEMORY_LIMIT)
    if memory_limit_match is None or float(memory_limit_match.group(1)) <= 0:
        raise ValueError(
            "DUCKDB_MEMORY_LIMIT deve ser uma quantidade positiva em B, KB, "
            "MB, GB ou TB."
        )

    connection = duckdb.connect(":memory:")
    try:
        connection.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
        connection.execute(f"SET threads = {DUCKDB_THREADS}")
        connection.execute(
            "SET preserve_insertion_order = "
            f"{str(DUCKDB_PRESERVE_INSERTION_ORDER).lower()}"
        )
        connection.execute("INSTALL quack")
        connection.execute("LOAD quack")
        connection.execute(
            """
            CALL quack_serve(
                ?,
                token = ?,
                disable_ssl = true,
                allow_other_hostname = true
            )
            """,
            [f"quack:{host}:{port}", token],
        )
        logger.info(
            "Servidor Quack ativo na porta %s (memória=%s, threads=%s).",
            port,
            DUCKDB_MEMORY_LIMIT,
            DUCKDB_THREADS,
        )
        Event().wait()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
