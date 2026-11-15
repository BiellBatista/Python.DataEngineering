"""Testes unitários da serialização de parâmetros SQL para DuckDB e Quack."""

import unittest
from datetime import date, datetime, timezone
from unittest.mock import Mock, call, patch

import duckdb

from utils import duckdb_connection as duckdb_module
from utils.duckdb_connection import (
    _sql_literal,
    execute_query,
)


class ExecuteQueryTests(unittest.TestCase):
    """Verifica a substituição segura de parâmetros para consultas DuckDB."""

    def setUp(self) -> None:
        """Cria uma conexão DuckDB local para cada caso de teste."""
        self.connection = duckdb.connect(":memory:")

    def tearDown(self) -> None:
        """Fecha a conexão criada para o caso de teste."""
        self.connection.close()

    def test_renders_strings_and_lists_as_sql_literals(self) -> None:
        """Escapa aspas e serializa listas como literais DuckDB."""
        result = execute_query(
            self.connection,
            "SELECT $name, $paths",
            {"name": "B3's data", "paths": ["bronze/a.parquet", "silver/b.parquet"]},
        ).fetchone()

        self.assertEqual(
            result,
            ("B3's data", ["bronze/a.parquet", "silver/b.parquet"]),
        )

    def test_leaves_placeholders_in_literals_and_comments_untouched(self) -> None:
        """Não substitui marcadores que façam parte de texto ou comentários."""
        query = """
            SELECT $value AS value, '$value' AS literal
            -- $value remains a comment
            /* $value also remains a comment */
        """

        result = execute_query(
            self.connection,
            query,
            {"value": "ok"},
        ).fetchone()

        self.assertEqual(result, ("ok", "$value"))

    def test_rejects_missing_and_unused_parameters(self) -> None:
        """Reporta marcadores ausentes e valores que não aparecem no SQL."""
        with self.assertRaisesRegex(ValueError, "não fornecido"):
            execute_query(self.connection, "SELECT $missing", {})

        with self.assertRaisesRegex(ValueError, "não utilizado"):
            execute_query(self.connection, "SELECT 1", {"unused": 1})

    def test_renders_dates_and_timezone_aware_timestamps(self) -> None:
        """Preserva tipos DATE e TIMESTAMPTZ durante a serialização."""
        result = execute_query(
            self.connection,
            "SELECT typeof($day), typeof($timestamp), CAST($day AS VARCHAR)",
            {
                "day": date(2026, 11, 6),
                "timestamp": datetime(2026, 11, 6, tzinfo=timezone.utc),
            },
        ).fetchone()

        self.assertEqual(result, ("DATE", "TIMESTAMP WITH TIME ZONE", "2026-11-06"))

    def test_rejects_non_finite_float_values(self) -> None:
        """Impede que NaN ou infinito sejam serializados em consultas SQL."""
        with self.assertRaisesRegex(ValueError, "finitos"):
            _sql_literal(float("nan"))

    def test_renders_scalar_values_and_nested_sequences(self) -> None:
        result = execute_query(
            self.connection,
            "SELECT $null_value, $boolean, $integer, $float, $values",
            {
                "null_value": None,
                "boolean": True,
                "integer": 3,
                "float": 1.25,
                "values": ("a'b", "second"),
            },
        ).fetchone()

        self.assertEqual(result, (None, True, 3, 1.25, ["a'b", "second"]))
        self.assertEqual(_sql_literal((False, "a'b", [2, None])), "[FALSE, 'a''b', [2, NULL]]")

    def test_renders_naive_timestamp_and_escaped_string_literal(self) -> None:
        self.assertEqual(
            _sql_literal(datetime(2026, 11, 6, 12, 30)),
            "TIMESTAMP '2026-11-06T12:30:00'",
        )
        self.assertEqual(_sql_literal("a'b"), "'a''b'")

    def test_rejects_unsupported_values_and_unclosed_sql(self) -> None:
        with self.assertRaisesRegex(TypeError, "Tipo não suportado"):
            _sql_literal(object())
        with self.assertRaisesRegex(ValueError, "finitos"):
            _sql_literal(float("inf"))
        with self.assertRaisesRegex(ValueError, "memory_limit"):
            duckdb_module._validate_memory_limit("unlimited")
        with self.assertRaisesRegex(ValueError, "string ou comentário aberto"):
            execute_query(self.connection, "SELECT 'open", {})
        with self.assertRaisesRegex(ValueError, "string ou comentário aberto"):
            execute_query(self.connection, "SELECT 1 /* open", {})

    def test_preserves_nested_comments_and_escaped_quotes(self) -> None:
        self.connection.execute('CREATE TABLE quoted ("$value" VARCHAR)')
        self.connection.execute("INSERT INTO quoted VALUES ('column value')")
        query = """
            SELECT $value, 'it''s $value' AS single_quote, "$value" AS double_quote
            FROM quoted
            /* outer /* nested $value */ outer */
            -- $value in comment
        """

        result = execute_query(
            self.connection,
            query,
            {"value": "ok"},
        ).fetchone()

        self.assertEqual(result, ("ok", "it's $value", "column value"))

    def test_local_connection_applies_and_validates_settings(self) -> None:
        connection = duckdb_module.connect_duckdb(
            memory_limit="2GB",
            threads=2,
            preserve_insertion_order=True,
        )
        try:
            result = connection.execute(
                "SELECT current_setting('threads'), "
                "current_setting('preserve_insertion_order')"
            ).fetchone()
            self.assertEqual(result, (2, True))
        finally:
            connection.close()

    def test_local_connection_rejects_invalid_memory_and_threads(self) -> None:
        connection = Mock()
        with patch.object(duckdb_module.duckdb, "connect", return_value=connection):
            with self.assertRaisesRegex(ValueError, "memory_limit"):
                duckdb_module.connect_duckdb(memory_limit="0GB")
            connection.close.assert_called_once()

        connection.reset_mock()
        with patch.object(duckdb_module.duckdb, "connect", return_value=connection):
            with self.assertRaisesRegex(ValueError, "threads"):
                duckdb_module.connect_duckdb(threads=0)
            connection.close.assert_called_once()

    def test_remote_connection_requires_token_and_matching_settings(self) -> None:
        cases = (
            ("1GB", None, False, "memory_limit"),
            (None, 1, False, "threads"),
            (None, None, True, "preserve_insertion_order"),
        )
        with (
            patch.object(duckdb_module, "DUCKDB_HOST", "duckdb"),
            patch.object(duckdb_module, "DUCKDB_MEMORY_LIMIT", "8GB"),
            patch.object(duckdb_module, "DUCKDB_THREADS", 4),
            patch.object(duckdb_module, "DUCKDB_PRESERVE_INSERTION_ORDER", False),
            patch.object(duckdb_module, "QUACK_TOKEN", "token"),
        ):
            for memory_limit, threads, preserve, message in cases:
                connection = Mock()
                with patch.object(
                    duckdb_module.duckdb,
                    "connect",
                    return_value=connection,
                ):
                    with self.subTest(message=message):
                        with self.assertRaisesRegex(ValueError, message):
                            duckdb_module.connect_duckdb(
                                memory_limit=memory_limit,
                                threads=threads,
                                preserve_insertion_order=preserve,
                            )
                connection.close.assert_called_once()

    def test_remote_connection_requires_token(self) -> None:
        connection = Mock()
        with (
            patch.object(duckdb_module, "DUCKDB_HOST", "duckdb"),
            patch.object(duckdb_module, "QUACK_TOKEN", None),
            patch.object(
                duckdb_module.duckdb,
                "connect",
                return_value=connection,
            ),
        ):
            with self.assertRaisesRegex(ValueError, "QUACK_TOKEN"):
                duckdb_module.connect_duckdb()
        connection.close.assert_called_once()

    def test_remote_connection_attaches_with_escaped_credentials(self) -> None:
        connection = Mock()
        with (
            patch.object(duckdb_module, "DUCKDB_HOST", "duck'host"),
            patch.object(duckdb_module, "DUCKDB_PORT", 9494),
            patch.object(duckdb_module, "DUCKDB_MEMORY_LIMIT", "8GB"),
            patch.object(duckdb_module, "DUCKDB_THREADS", 4),
            patch.object(duckdb_module, "DUCKDB_PRESERVE_INSERTION_ORDER", False),
            patch.object(duckdb_module, "QUACK_TOKEN", "token'quoted"),
            patch.object(
                duckdb_module.duckdb,
                "connect",
                return_value=connection,
            ),
        ):
            result = duckdb_module.connect_duckdb()

        self.assertIs(result, connection)
        self.assertEqual(
            connection.execute.call_args_list,
            [
                call("INSTALL quack"),
                call("LOAD quack"),
                call(
                    "ATTACH 'quack:duck''host:9494' AS remote "
                    "(TYPE quack, TOKEN 'token''quoted', DISABLE_SSL true)"
                ),
                call("CONNECT remote"),
            ],
        )

    def test_remote_connection_closes_if_quack_setup_fails(self) -> None:
        connection = Mock()
        connection.execute.side_effect = RuntimeError("extension unavailable")
        with (
            patch.object(duckdb_module, "DUCKDB_HOST", "duckdb"),
            patch.object(duckdb_module, "DUCKDB_MEMORY_LIMIT", "8GB"),
            patch.object(duckdb_module, "DUCKDB_THREADS", 4),
            patch.object(duckdb_module, "DUCKDB_PRESERVE_INSERTION_ORDER", False),
            patch.object(duckdb_module, "QUACK_TOKEN", "token"),
            patch.object(
                duckdb_module.duckdb,
                "connect",
                return_value=connection,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "extension unavailable"):
                duckdb_module.connect_duckdb()
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
