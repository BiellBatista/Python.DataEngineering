"""Testes unitários da serialização de parâmetros SQL para DuckDB e Quack."""

import unittest
from datetime import date, datetime, timezone

import duckdb

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


if __name__ == "__main__":
    unittest.main()
