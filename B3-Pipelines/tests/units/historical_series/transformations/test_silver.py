"""Testes de integração da transformação Silver."""

import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

import duckdb

from common.exceptions import BronzeError, SilverError
from historical_series.transformations import silver
from historical_series.transformations.silver import execute_silver_load


class SilverTransformationTests(unittest.TestCase):
    """Verifica metadados, precisão decimal e deduplicação no Parquet Silver."""

    def test_load_preserves_lineage_and_selects_deterministic_decimal_row(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bronze_partition = (
                root
                / "bronze"
                / "year=2024"
                / "_ingested_date=2024-01-03_00-00-00"
            )
            bronze_partition.mkdir(parents=True)
            bronze_file = bronze_partition / "part.parquet"

            bronze_connection = duckdb.connect(":memory:")
            try:
                bronze_connection.execute(
                    """
                    CREATE TABLE bronze (
                        content VARCHAR,
                        _ingested_at TIMESTAMPTZ,
                        _source_file VARCHAR,
                        _dag_run_id VARCHAR
                    )
                    """
                )
                bronze_connection.executemany(
                    "INSERT INTO bronze VALUES (?, ?, ?, ?)",
                    [
                        (
                            _cotahist_record("9999"),
                            datetime(2024, 1, 2, tzinfo=timezone.utc),
                            "COTAHIST_Z_OLD.TXT",
                            "bronze-run",
                        ),
                        (
                            _cotahist_record("1234"),
                            datetime(2024, 1, 3, tzinfo=timezone.utc),
                            "COTAHIST_A.TXT",
                            "bronze-run",
                        ),
                        (
                            _cotahist_record("5678"),
                            datetime(2024, 1, 3, tzinfo=timezone.utc),
                            "COTAHIST_Z.TXT",
                            "bronze-run",
                        ),
                    ],
                )
                bronze_connection.execute(
                    f"COPY bronze TO '{_escape_sql_path(bronze_file)}' "
                    "(FORMAT PARQUET)"
                )
            finally:
                bronze_connection.close()

            silver_root = root / "silver"
            execute_silver_load(
                dag_run_id="silver-run",
                start_year=2024,
                end_year=2024,
                memory_limit=None,
                threads=1,
                input_dir=str(root / "bronze"),
                output_dir=str(silver_root),
            )

            silver_file = next((silver_root / "year=2024").glob("*.parquet"))
            silver_connection = duckdb.connect(":memory:")
            try:
                result = silver_connection.execute(
                    "SELECT preco_fechamento, volume_total, "
                    "CAST(_ingested_at AS VARCHAR), "
                    "CAST(_bronze_ingested_at AS VARCHAR), _raw_source_file, "
                    "_bronze_source_file, _dag_run_id "
                    f"FROM read_parquet('{_escape_sql_path(silver_file)}')"
                ).fetchone()
                types = dict(
                    (row[0], row[1])
                    for row in silver_connection.execute(
                        f"DESCRIBE SELECT * FROM read_parquet('{_escape_sql_path(silver_file)}')"
                    ).fetchall()
                )
            finally:
                silver_connection.close()

            self.assertEqual(result[0], Decimal("56.78"))
            self.assertEqual(result[1], Decimal("1234.56"))
            self.assertGreater(result[2], result[3])
            self.assertEqual(result[4], "COTAHIST_Z.TXT")
            self.assertEqual(result[5], str(bronze_file))
            self.assertEqual(result[6], "silver-run")
            self.assertNotIn("_source_file", types)
            for column in (
                "preco_abertura",
                "preco_maximo",
                "preco_minimo",
                "preco_fechamento",
                "volume_total",
            ):
                self.assertEqual(types[column], "DECIMAL(18,2)")
            self.assertEqual(types["_ingested_at"], "TIMESTAMP WITH TIME ZONE")

    def test_load_rejects_invalid_years_threads_and_memory_limit(self) -> None:
        cases = (
            ({"start_year": 2025, "end_year": 2024}, "start_year"),
            ({"start_year": 2024, "end_year": 2024, "threads": 0}, "threads"),
            (
                {"start_year": 2024, "end_year": 2024, "memory_limit": "invalid"},
                "memory_limit",
            ),
        )
        for parameters, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    execute_silver_load(
                        dag_run_id="run",
                        input_dir="/unused",
                        output_dir="/unused",
                        **parameters,
                    )

    def test_load_rejects_file_destination_and_missing_bronze_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            output_file = root / "silver"
            output_file.touch()
            with self.assertRaises(SilverError):
                execute_silver_load(
                    "run", 2024, 2024, threads=1, input_dir=str(root), output_dir=str(output_file)
                )

            with (
                patch.object(silver, "list_bronze_dirs", return_value=["year=2024"]),
                patch.object(silver, "check_directories_exist", return_value=["year=2024"]),
            ):
                with self.assertRaises(BronzeError):
                    execute_silver_load(
                        "run",
                        2024,
                        2024,
                        threads=1,
                        input_dir=str(root),
                        output_dir=str(root / "silver-output"),
                    )

    def test_load_rejects_missing_parquets_and_closes_failed_connection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            output = root / "silver"
            with (
                patch.object(silver, "list_bronze_dirs", return_value=["year=2024"]),
                patch.object(silver, "check_directories_exist", return_value=[]),
                patch.object(silver, "find_most_recent_bronze_parquets", return_value=[]),
            ):
                with self.assertRaisesRegex(BronzeError, "Nenhum arquivo Parquet"):
                    execute_silver_load(
                        "run", 2024, 2024, threads=1, input_dir=str(root), output_dir=str(output)
                    )

            connection = Mock()
            with (
                patch.object(silver, "list_bronze_dirs", return_value=["year=2024"]),
                patch.object(silver, "check_directories_exist", return_value=[]),
                patch.object(
                    silver,
                    "find_most_recent_bronze_parquets",
                    return_value=["input.parquet"],
                ),
                patch.object(silver, "connect_duckdb", return_value=connection),
                patch.object(
                    silver,
                    "execute_query",
                    side_effect=RuntimeError("query failed"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "query failed"):
                    execute_silver_load(
                        "run", 2024, 2024, threads=1, input_dir=str(root), output_dir=str(output)
                    )
            connection.close.assert_called_once()
            self.assertEqual(list(output.iterdir()), [])

    def test_load_uses_cpu_fallback_and_promotes_staged_partition(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            output = root / "silver"
            connection = Mock()

            def write_staged_partition(_connection, _query, parameters):
                (Path(parameters["output_dir"]) / "year=2024").mkdir()

            with (
                patch.object(silver, "list_bronze_dirs", return_value=["year=2024"]),
                patch.object(silver, "check_directories_exist", return_value=[]),
                patch.object(
                    silver,
                    "find_most_recent_bronze_parquets",
                    return_value=["input.parquet"],
                ),
                patch.object(silver, "connect_duckdb", return_value=connection) as connect,
                patch.object(silver, "execute_query", side_effect=write_staged_partition),
                patch.object(silver.os, "cpu_count", return_value=None),
            ):
                execute_silver_load(
                    "run",
                    2024,
                    2024,
                    memory_limit=None,
                    threads=None,
                    input_dir=str(root),
                    output_dir=str(output),
                )

            connect.assert_called_once_with(memory_limit=None, threads=4)
            connection.close.assert_called_once()
            self.assertTrue((output / "year=2024").is_dir())

    def test_load_rejects_missing_staged_partition_and_cleans_temporary_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            output = root / "silver"
            connection = Mock()
            with (
                patch.object(silver, "list_bronze_dirs", return_value=["year=2024"]),
                patch.object(silver, "check_directories_exist", return_value=[]),
                patch.object(
                    silver,
                    "find_most_recent_bronze_parquets",
                    return_value=["input.parquet"],
                ),
                patch.object(silver, "connect_duckdb", return_value=connection),
                patch.object(silver, "execute_query"),
            ):
                with self.assertRaisesRegex(SilverError, "não gerou a partição"):
                    execute_silver_load(
                        "run",
                        2024,
                        2024,
                        threads=1,
                        input_dir=str(root),
                        output_dir=str(output),
                    )

            connection.close.assert_called_once()
            self.assertEqual(list(output.iterdir()), [])

    def test_promote_output_directory_replaces_existing_and_rolls_back_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            staged = root / "staged"
            destination = root / "destination"
            staged.mkdir()
            (staged / "new.parquet").touch()
            destination.mkdir()
            (destination / "old.parquet").touch()

            silver._promote_output_directory(staged, destination)
            self.assertTrue((destination / "new.parquet").is_file())
            self.assertFalse((destination / "old.parquet").exists())

            staged.mkdir()
            (staged / "replacement.parquet").touch()
            original_replace = silver.os.replace
            calls = 0

            def fail_promotion_once(source, target):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("promotion failed")
                original_replace(source, target)

            with patch.object(silver.os, "replace", side_effect=fail_promotion_once):
                with self.assertRaisesRegex(OSError, "promotion failed"):
                    silver._promote_output_directory(staged, destination)

            self.assertTrue((destination / "new.parquet").is_file())
            self.assertTrue((staged / "replacement.parquet").is_file())

    def test_promote_output_directory_without_previous_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            staged = root / "staged"
            destination = root / "destination"
            staged.mkdir()
            silver._promote_output_directory(staged, destination)
            self.assertTrue(destination.is_dir())
            self.assertFalse(staged.exists())


def _cotahist_record(close_price: str) -> str:
    record = [" "] * 245
    fields = (
        (0, "01"),
        (2, "20240102"),
        (12, "TEST3".ljust(12)),
        (24, "010"),
        (56, "0000000001234"),
        (69, "0000000005678"),
        (82, "0000000001000"),
        (108, close_price.zfill(13)),
        (170, "000000000000123456"),
    )
    for start, value in fields:
        record[start : start + len(value)] = value
    return "".join(record)


def _escape_sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


if __name__ == "__main__":
    unittest.main()
