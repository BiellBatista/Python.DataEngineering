"""Testes da ingestão RAW para Parquet Bronze."""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from common.exceptions import BronzeError, RawError
from historical_series.ingestions.raw import get_raw_dir
from historical_series.transformations import bronze


class BronzeTransformationTests(unittest.TestCase):
    def test_lists_bronze_partitions_inclusive_and_rejects_reversed_range(self) -> None:
        self.assertEqual(
            bronze.list_bronze_dirs(2023, 2024, "/lake"),
            ["/lake/year=2023", "/lake/year=2024"],
        )
        with self.assertRaisesRegex(ValueError, "menor ou igual"):
            bronze.list_bronze_dirs(2025, 2024, "/lake")

    def test_finds_latest_ingestion_and_legacy_parquets(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            year_dir = Path(temporary_directory) / "year=2024"
            older = year_dir / "_ingested_date=2024-01-01_00-00-00"
            latest = year_dir / "_ingested_date=2024-01-02_00-00-00"
            older.mkdir(parents=True)
            latest.mkdir()
            old_file = older / "old.parquet"
            latest_file = latest / "new.parquet"
            old_file.touch()
            latest_file.touch()

            self.assertEqual(
                bronze.find_most_recent_bronze_parquets(str(year_dir)),
                [str(latest_file)],
            )

            legacy_file = year_dir / "legacy.parquet"
            legacy_file.touch()
            old_file.unlink()
            latest_file.unlink()
            older.rmdir()
            latest.rmdir()
            self.assertEqual(
                bronze.find_most_recent_bronze_parquets(str(year_dir)),
                [str(legacy_file)],
            )

    def test_load_rejects_invalid_arguments_before_touching_filesystem(self) -> None:
        cases = (
            ({"start_year": 2025, "end_year": 2024}, "start_year"),
            ({"start_year": 2024, "end_year": 2024, "threads": 0}, "threads"),
            (
                {"start_year": 2024, "end_year": 2024, "max_workers": 0},
                "max_workers",
            ),
            (
                {
                    "start_year": 2024,
                    "end_year": 2024,
                    "memory_limit": "invalid",
                },
                "memory_limit",
            ),
        )
        for parameters, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    bronze.execute_bronze_load(
                        dag_run_id="run",
                        input_dir="/unused",
                        output_dir="/unused",
                        **parameters,
                    )

    def test_load_requires_raw_files_and_validates_workers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)

            with self.assertRaises(RawError):
                bronze.execute_bronze_load(
                    "run", 2024, 2024, input_dir=str(root), output_dir=str(root / "out")
                )

            with patch.object(bronze, "list_raw_dirs", return_value=[]):
                with self.assertRaisesRegex(BronzeError, "Nenhum arquivo RAW"):
                    bronze.execute_bronze_load(
                        "run", 2024, 2024, input_dir=str(root), output_dir=str(root / "out")
                    )

    def test_load_splits_local_threads_across_workers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            for year in (2023, 2024):
                raw_file = Path(get_raw_dir(year, root))
                raw_file.parent.mkdir(parents=True, exist_ok=True)
                raw_file.touch()

            with patch.object(bronze, "_load_raw_dir") as load_raw_dir:
                bronze.execute_bronze_load(
                    "run",
                    2023,
                    2024,
                    threads=4,
                    max_workers=2,
                    input_dir=str(root),
                    output_dir=str(root / "bronze"),
                    ingestion_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                )

        self.assertEqual(load_raw_dir.call_count, 2)
        self.assertEqual(
            [call_args.args[4] for call_args in load_raw_dir.call_args_list],
            [2, 2],
        )
        self.assertTrue(
            all(call_args.args[5].year == 2024 for call_args in load_raw_dir.call_args_list)
        )

    def test_load_uses_cpu_default_and_remote_worker_settings(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            for year in (2023, 2024):
                raw_file = Path(get_raw_dir(year, root))
                raw_file.parent.mkdir(parents=True, exist_ok=True)
                raw_file.touch()

            with (
                patch.object(bronze.os, "cpu_count", return_value=6),
                patch.object(bronze, "DUCKDB_HOST", "duckdb-server"),
                patch.object(bronze, "_load_raw_dir") as load_raw_dir,
            ):
                bronze.execute_bronze_load(
                    "run",
                    2023,
                    2024,
                    threads=None,
                    max_workers=None,
                    input_dir=str(root),
                    output_dir=str(root / "bronze"),
                )

        self.assertEqual(load_raw_dir.call_count, 2)
        self.assertEqual(
            [call_args.args[4] for call_args in load_raw_dir.call_args_list],
            [6, 6],
        )

    def test_load_propagates_worker_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            raw_file = Path(get_raw_dir(2024, root))
            raw_file.parent.mkdir(parents=True)
            raw_file.touch()

            with patch.object(
                bronze,
                "_load_raw_dir",
                side_effect=RuntimeError("worker failed"),
            ):
                with self.assertRaisesRegex(RuntimeError, "worker failed"):
                    bronze.execute_bronze_load(
                        "run",
                        2024,
                        2024,
                        input_dir=str(root),
                        output_dir=str(root / "bronze"),
                    )

    def test_worker_closes_connection_on_success_and_failure(self) -> None:
        connection = Mock()
        with (
            patch.object(bronze, "connect_duckdb", return_value=connection),
            patch.object(bronze, "execute_query"),
        ):
            bronze._load_raw_dir(
                "raw.txt",
                "run",
                "out",
                None,
                2,
                datetime(2024, 1, 1, tzinfo=timezone.utc),
            )
        connection.close.assert_called_once()

        connection.reset_mock()
        with (
            patch.object(bronze, "connect_duckdb", return_value=connection),
            patch.object(bronze, "execute_query", side_effect=RuntimeError("copy failed")),
        ):
            with self.assertRaisesRegex(RuntimeError, "copy failed"):
                bronze._load_raw_dir(
                    "raw.txt",
                    "run",
                    "out",
                    None,
                    2,
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                )
        connection.close.assert_called_once()

    def test_worker_does_not_close_connection_when_connect_fails(self) -> None:
        with patch.object(bronze, "connect_duckdb", side_effect=RuntimeError("connect")):
            with self.assertRaisesRegex(RuntimeError, "connect"):
                bronze._load_raw_dir(
                    "raw.txt",
                    "run",
                    "out",
                    None,
                    2,
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                )

    def test_memory_limit_validator(self) -> None:
        bronze._validate_memory_limit(None)
        bronze._validate_memory_limit(" 1.5 gb ")
        with self.assertRaisesRegex(ValueError, "memory_limit"):
            bronze._validate_memory_limit("0GB")
        with self.assertRaisesRegex(ValueError, "memory_limit"):
            bronze._validate_memory_limit("invalid")


if __name__ == "__main__":
    unittest.main()