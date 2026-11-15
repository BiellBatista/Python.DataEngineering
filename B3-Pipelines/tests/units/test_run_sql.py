"""Testes da CLI SQL interativa."""

import builtins
import importlib
import runpy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, call, patch

import duckdb

import run_sql


class RunSqlTests(unittest.TestCase):
    def test_repl_handles_shortcuts_queries_blank_input_and_exit(self) -> None:
        connection = Mock()
        connection.sql.return_value = connection.sql
        with (
            patch("run_sql.connect_duckdb", return_value=connection),
            patch("run_sql._register_parquet_views") as register_views,
            patch("builtins.input", side_effect=["", "schema", "count", "select 1", "exit"]),
            patch("builtins.print"),
        ):
            run_sql.start_interactive_cli("bronze-dir", "silver-dir")

        register_views.assert_called_once_with(connection, "bronze-dir", "silver-dir")
        self.assertEqual(
            connection.sql.call_args_list,
            [
                call("DESCRIBE silver;"),
                call("SELECT count(*) AS total_registros FROM silver;"),
                call("select 1"),
            ],
        )
        self.assertEqual(connection.sql.return_value.show.call_count, 3)
        connection.close.assert_called_once()

    def test_repl_reports_sql_errors_and_continues(self) -> None:
        connection = Mock()
        connection.sql.side_effect = [
            RuntimeError("invalid SQL"),
            connection.sql.return_value,
        ]
        with (
            patch("run_sql.connect_duckdb", return_value=connection),
            patch("run_sql._register_parquet_views"),
            patch("builtins.input", side_effect=["select broken", "exit"]),
            patch("builtins.print") as print_output,
        ):
            run_sql.start_interactive_cli()

        self.assertTrue(
            any("Erro na execução do SQL" in str(call) for call in print_output.call_args_list)
        )
        connection.close.assert_called_once()

    def test_repl_handles_interrupt_and_eof(self) -> None:
        connection = Mock()
        with (
            patch("run_sql.connect_duckdb", return_value=connection),
            patch("run_sql._register_parquet_views"),
            patch(
                "builtins.input",
                side_effect=[KeyboardInterrupt(), EOFError()],
            ),
            patch("builtins.print") as print_output,
        ):
            run_sql.start_interactive_cli()

        output = " ".join(str(item) for item in print_output.call_args_list)
        self.assertIn("Operação cancelada", output)
        self.assertIn("Até logo", output)
        connection.close.assert_called_once()

    def test_repl_reports_view_setup_error_and_closes_connection(self) -> None:
        connection = Mock()
        with (
            patch("run_sql.connect_duckdb", return_value=connection),
            patch(
                "run_sql._register_parquet_views",
                side_effect=FileNotFoundError("missing parquet"),
            ),
            patch("builtins.print") as print_output,
        ):
            run_sql.start_interactive_cli()

        self.assertTrue(
            any(
                "Erro ao registrar os arquivos Parquet" in str(call)
                for call in print_output.call_args_list
            )
        )
        connection.close.assert_called_once()

    def test_registers_bronze_silver_and_legacy_views(self) -> None:
        with tempfile.TemporaryDirectory(prefix="b3's-lake-") as directory:
            root = Path(directory)
            bronze_path = root / "bronze"
            silver_path = root / "silver"
            bronze_path.mkdir()
            silver_path.mkdir()
            _write_parquet(bronze_path / "year=2024" / "part.parquet", "bronze")
            _write_parquet(silver_path / "year=2024" / "part.parquet", "silver")

            connection = duckdb.connect(":memory:")
            try:
                run_sql._register_parquet_views(
                    connection,
                    str(bronze_path),
                    str(silver_path),
                )
                counts = connection.execute(
                    "SELECT (SELECT count(*) FROM bronze), "
                    "(SELECT count(*) FROM silver), "
                    "(SELECT count(*) FROM cotacoes)"
                ).fetchone()
            finally:
                connection.close()

        self.assertEqual(counts, (1, 1, 1))

    def test_register_views_requires_parquet_for_each_layer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bronze_path = root / "bronze"
            silver_path = root / "silver"
            bronze_path.mkdir()
            silver_path.mkdir()
            connection = duckdb.connect(":memory:")
            try:
                with self.assertRaisesRegex(FileNotFoundError, "bronze"):
                    run_sql._register_parquet_views(
                        connection, str(bronze_path), str(silver_path)
                    )

                _write_parquet(bronze_path / "part.parquet", "bronze")
                with self.assertRaisesRegex(FileNotFoundError, "silver"):
                    run_sql._register_parquet_views(
                        connection, str(bronze_path), str(silver_path)
                    )
            finally:
                connection.close()

    def test_readline_import_fallback(self) -> None:
        original_import = builtins.__import__

        def import_without_readline(name, *args, **kwargs):
            if name == "readline":
                raise ImportError("not available")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=import_without_readline):
            importlib.reload(run_sql)

        importlib.reload(run_sql)

    def test_module_entry_point_starts_interactive_cli(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bronze_path = root / "bronze"
            silver_path = root / "silver"
            bronze_path.mkdir()
            silver_path.mkdir()
            _write_parquet(bronze_path / "part.parquet", "bronze")
            _write_parquet(silver_path / "part.parquet", "silver")

        connection = Mock()
        with (
            patch("config.BRONZE_COTAHIST_DIR", str(bronze_path)),
            patch("config.SILVER_COTAHIST_DIR", str(silver_path)),
            patch("utils.duckdb_connection.connect_duckdb", return_value=connection),
            patch("builtins.input", return_value="exit"),
            patch("builtins.print"),
        ):
            runpy.run_path(run_sql.__file__, run_name="__main__")
        connection.close.assert_called_once()


def _write_parquet(path: Path, layer: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    escaped_path = str(path).replace("'", "''")
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            f"COPY (SELECT '{layer}' AS layer) TO '{escaped_path}' (FORMAT PARQUET)"
        )
    finally:
        connection.close()


if __name__ == "__main__":
    unittest.main()
