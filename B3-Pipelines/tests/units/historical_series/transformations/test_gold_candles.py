"""Testes da transformação de candles da camada Gold."""

import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

import duckdb

from common.exceptions import GoldError
from historical_series.transformations import gold_candles
from historical_series.transformations.gold_candles import execute_gold_candles_load


class GoldTransformationTests(unittest.TestCase):
    def test_load_filters_valid_candles_by_prefix_and_replaces_only_selected_tickers(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver_root = root / "silver"
            silver_root.mkdir()
            silver_file = silver_root / "year=2024" / "part.parquet"
            silver_file.parent.mkdir()
            _write_silver(silver_file)
            gold_root = root / "gold"

            execute_gold_candles_load(
                start_with="VALE",
                dag_run_id="test-run-1",
                memory_limit=None,
                threads=1,
                input_dir=str(silver_root),
                output_dir=str(gold_root),
            )

            vale3_partition = gold_root / "ticker=VALE3"
            vale5_partition = gold_root / "ticker=VALE5"
            self.assertTrue(vale3_partition.is_dir())
            self.assertTrue(vale5_partition.is_dir())
            self.assertFalse((gold_root / "ticker=PETR4").exists())

            connection = duckdb.connect(":memory:")
            try:
                file_path = next(vale3_partition.glob("*.parquet"))
                rows = connection.execute(
                    "SELECT ticker, timeframe, time, tipo_mercado, open, high, "
                    "low, close, volume_total, _ingested_at, _dag_run_id "
                    "FROM read_parquet(?, hive_partitioning = true) "
                    "ORDER BY timeframe, time",
                    [str(file_path)],
                ).fetchall()
                types = {
                    row[0]: row[1]
                    for row in connection.execute(
                        "DESCRIBE SELECT * FROM read_parquet(?, "
                        "hive_partitioning = true)",
                        [str(file_path)],
                    ).fetchall()
                }
            finally:
                connection.close()

            self.assertTrue(rows)
            self.assertTrue(
                all(isinstance(row[9], datetime) for row in rows)
            )
            self.assertTrue(all(row[10] == "test-run-1" for row in rows))
            rows = [row[:9] + row[10:] for row in rows]

            self.assertEqual(
                rows,
                [
                    (
                        "VALE3",
                        "1d",
                        date(2024, 1, 2),
                        "010",
                        Decimal("10.00"),
                        Decimal("12.00"),
                        Decimal("9.00"),
                        Decimal("11.00"),
                        Decimal("100.00"),
                        "test-run-1",
                    ),
                    (
                        "VALE3",
                        "1d",
                        date(2024, 1, 31),
                        "010",
                        Decimal("20.00"),
                        Decimal("25.00"),
                        Decimal("18.00"),
                        Decimal("22.00"),
                        Decimal("200.00"),
                        "test-run-1",
                    ),
                    (
                        "VALE3",
                        "1d",
                        date(2024, 2, 5),
                        "010",
                        Decimal("30.00"),
                        Decimal("35.00"),
                        Decimal("28.00"),
                        Decimal("33.00"),
                        Decimal("300.00"),
                        "test-run-1",
                    ),
                    (
                        "VALE3",
                        "1m",
                        date(2024, 1, 1),
                        "010",
                        Decimal("10.00"),
                        Decimal("25.00"),
                        Decimal("9.00"),
                        Decimal("22.00"),
                        Decimal("300.00"),
                        "test-run-1",
                    ),
                    (
                        "VALE3",
                        "1m",
                        date(2024, 2, 1),
                        "010",
                        Decimal("30.00"),
                        Decimal("35.00"),
                        Decimal("28.00"),
                        Decimal("33.00"),
                        Decimal("300.00"),
                        "test-run-1",
                    ),
                ],
            )
            self.assertEqual(
                {name: types[name] for name in ("open", "high", "low", "close")},
                {name: "DECIMAL(18,2)" for name in ("open", "high", "low", "close")},
            )
            self.assertEqual(types["timeframe"], "VARCHAR")
            self.assertEqual(types["_ingested_at"], "TIMESTAMP")
            self.assertEqual(types["_dag_run_id"], "VARCHAR")

            petr_partition = gold_root / "ticker=PETR4"
            petr_partition.mkdir()
            (petr_partition / "preserved").touch()
            execute_gold_candles_load(
                start_with="VALE",
                memory_limit=None,
                threads=1,
                input_dir=str(silver_root),
                output_dir=str(gold_root),
            )
            self.assertTrue((petr_partition / "preserved").is_file())
            self.assertTrue(vale5_partition.is_dir())
            self.assertTrue(next(vale3_partition.glob("*.parquet")).is_file())

    def test_load_supports_prefix_lists_and_defaults_to_all_tickers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver_root = root / "silver"
            silver_root.mkdir()
            _write_silver(silver_root / "part.parquet")
            prefix_output = root / "gold-prefix"
            execute_gold_candles_load(
                start_with=["PETR"],
                memory_limit=None,
                threads=1,
                input_dir=str(silver_root),
                output_dir=str(prefix_output),
            )
            self.assertTrue((prefix_output / "ticker=PETR4").is_dir())
            self.assertFalse((prefix_output / "ticker=VALE3").exists())

            all_output = root / "gold-all"
            execute_gold_candles_load(
                memory_limit=None,
                threads=1,
                input_dir=str(silver_root),
                output_dir=str(all_output),
            )
            self.assertTrue((all_output / "ticker=VALE3").is_dir())
            self.assertTrue((all_output / "ticker=VALE5").is_dir())
            self.assertTrue((all_output / "ticker=PETR4").is_dir())

    def test_load_rejects_empty_prefix_results(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver_root = root / "silver"
            silver_root.mkdir()
            silver_file = silver_root / "part.parquet"
            _write_silver(silver_file)
            with self.assertRaisesRegex(GoldError, "Nenhum ticker selecionado"):
                execute_gold_candles_load(
                    start_with=["UNKNOWN"],
                    memory_limit=None,
                    threads=1,
                    input_dir=str(silver_root),
                    output_dir=str(root / "gold"),
                )

    def test_load_rejects_missing_silver_parquets_and_file_destination(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with self.assertRaisesRegex(GoldError, "não é um diretório"):
                execute_gold_candles_load(
                    input_dir=str(root / "missing"),
                    output_dir=str(root / "gold"),
                )

            silver_root = root / "silver"
            silver_root.mkdir()
            with self.assertRaisesRegex(GoldError, "Nenhum arquivo Parquet"):
                execute_gold_candles_load(
                    input_dir=str(silver_root),
                    output_dir=str(root / "gold"),
                )

            _write_silver(silver_root / "part.parquet")
            output_file = root / "gold-file"
            output_file.touch()
            with self.assertRaisesRegex(GoldError, "não é um diretório"):
                execute_gold_candles_load(
                    memory_limit=None,
                    threads=1,
                    input_dir=str(silver_root),
                    output_dir=str(output_file),
                )

    def test_load_rejects_invalid_selectors_and_duckdb_settings(self) -> None:
        invalid_arguments = (
            ({"start_with": " "}, "start_with"),
            ({"start_with": ["VALE", " "]}, "start_with"),
            ({"threads": 0}, "threads"),
            ({"max_workers": 0}, "max_workers"),
            ({"memory_limit": "invalid"}, "memory_limit"),
        )
        for arguments, message in invalid_arguments:
            with self.subTest(arguments=arguments):
                with self.assertRaisesRegex(ValueError, message):
                    execute_gold_candles_load(
                        input_dir="/unused",
                        output_dir="/unused",
                        **arguments,
                    )

    def test_load_supports_selector_lists_and_remote_thread_settings(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver_root = root / "silver"
            silver_root.mkdir()
            _write_silver(silver_root / "part.parquet")
            gold_root = root / "gold"

            def fake_chunk(tickers, _input_files, output_dir, *_args):
                Path(output_dir).mkdir(parents=True, exist_ok=True)
                for ticker in tickers:
                    (Path(output_dir) / f"ticker={ticker}").mkdir()

            with (
                patch.object(gold_candles, "DUCKDB_HOST", "duckdb-server"),
                patch.object(
                    gold_candles, "_load_ticker_chunk", side_effect=fake_chunk
                ) as load_chunk,
            ):
                execute_gold_candles_load(
                    start_with=["VALE", "PETR"],
                    max_workers=2,
                    threads=4,
                    memory_limit=None,
                    input_dir=str(silver_root),
                    output_dir=str(gold_root),
                )

            self.assertTrue((gold_root / "ticker=VALE3").is_dir())
            self.assertTrue((gold_root / "ticker=VALE5").is_dir())
            self.assertTrue((gold_root / "ticker=PETR4").is_dir())
            self.assertEqual(load_chunk.call_count, 2)
            self.assertTrue(
                all(call.args[6] == 4 for call in load_chunk.call_args_list)
            )

    def test_load_splits_local_threads_across_ticker_workers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver_root = root / "silver"
            silver_root.mkdir()
            _write_silver(silver_root / "part.parquet")
            gold_root = root / "gold"

            def fake_chunk(tickers, _input_files, output_dir, *_args):
                Path(output_dir).mkdir(parents=True, exist_ok=True)
                for ticker in tickers:
                    (Path(output_dir) / f"ticker={ticker}").mkdir()

            with patch.object(
                gold_candles, "_load_ticker_chunk", side_effect=fake_chunk
            ) as load_chunk:
                execute_gold_candles_load(
                    start_with=["VALE", "PETR"],
                    max_workers=2,
                    threads=4,
                    memory_limit=None,
                    input_dir=str(silver_root),
                    output_dir=str(gold_root),
                )

            self.assertEqual(load_chunk.call_count, 2)
            self.assertTrue(
                all(call.args[6] == 2 for call in load_chunk.call_args_list)
            )
            self.assertTrue((gold_root / "ticker=VALE3").is_dir())
            self.assertTrue((gold_root / "ticker=VALE5").is_dir())
            self.assertTrue((gold_root / "ticker=PETR4").is_dir())

    def test_load_propagates_ticker_worker_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver_root = root / "silver"
            silver_root.mkdir()
            _write_silver(silver_root / "part.parquet")

            with patch.object(
                gold_candles,
                "_load_ticker_chunk",
                side_effect=RuntimeError("worker failed"),
            ):
                with self.assertRaisesRegex(RuntimeError, "worker failed"):
                    execute_gold_candles_load(
                        start_with="VALE3",
                        memory_limit=None,
                        threads=1,
                        input_dir=str(silver_root),
                        output_dir=str(root / "gold"),
                    )

    def test_load_fails_when_workers_generate_no_ticker_partitions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver_root = root / "silver"
            silver_root.mkdir()
            _write_silver(silver_root / "part.parquet")

            with patch.object(gold_candles, "_load_ticker_chunk"):
                with self.assertRaisesRegex(GoldError, "Nenhum ticker selecionado"):
                    execute_gold_candles_load(
                        start_with="VALE3",
                        memory_limit=None,
                        threads=1,
                        input_dir=str(silver_root),
                        output_dir=str(root / "gold"),
                    )

    def test_ticker_chunk_closes_connection_on_success_and_failure(self) -> None:
        connection = Mock()
        with (
            patch.object(gold_candles, "connect_duckdb", return_value=connection),
            patch.object(gold_candles, "execute_query"),
        ):
            gold_candles._load_ticker_chunk(
                ["VALE3"],
                ["input.parquet"],
                Path("/tmp/gold-staging"),
                "run",
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                None,
                1,
            )
        connection.close.assert_called_once()

        connection.reset_mock()
        with (
            patch.object(gold_candles, "connect_duckdb", return_value=connection),
            patch.object(
                gold_candles,
                "execute_query",
                side_effect=RuntimeError("copy failed"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "copy failed"):
                gold_candles._load_ticker_chunk(
                    ["VALE3"],
                    ["input.parquet"],
                    Path("/tmp/gold-staging"),
                    "run",
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    None,
                    1,
                )
        connection.close.assert_called_once()

    def test_promotion_restores_existing_ticker_partition_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            staging = root / "staging"
            destination = root / "ticker=VALE3"
            staging.mkdir()
            (staging / "new.parquet").touch()
            destination.mkdir()
            (destination / "old.parquet").touch()
            original_replace = gold_candles.os.replace
            calls = 0

            def fail_promotion_once(source, target):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("promotion failed")
                original_replace(source, target)

            with patch.object(
                gold_candles.os, "replace", side_effect=fail_promotion_once
            ):
                with self.assertRaisesRegex(OSError, "promotion failed"):
                    gold_candles._promote_ticker_partition(staging, destination)

            self.assertTrue((destination / "old.parquet").is_file())
            self.assertTrue((staging / "new.parquet").is_file())


def _write_silver(path: Path) -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """
            CREATE TABLE silver (
                ticker VARCHAR,
                data_pregao DATE,
                tipo_mercado VARCHAR,
                preco_abertura DECIMAL(18, 2),
                preco_maximo DECIMAL(18, 2),
                preco_minimo DECIMAL(18, 2),
                preco_fechamento DECIMAL(18, 2),
                volume_total DECIMAL(18, 2),
                _ingested_at TIMESTAMP,
                _bronze_ingested_at TIMESTAMP,
                _raw_source_file VARCHAR,
                _bronze_source_file VARCHAR,
                _dag_run_id VARCHAR
            )
            """
        )
        silver_metadata = (
            datetime(2024, 2, 1, tzinfo=timezone.utc),
            datetime(2024, 2, 1, tzinfo=timezone.utc),
            "COTAHIST_A2024.TXT",
            "bronze/year=2024/part.parquet",
            "silver-run-1",
        )
        connection.executemany(
            "INSERT INTO silver VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("VALE3", date(2024, 1, 2), "010", 10, 12, 9, 11, 100, *silver_metadata),
                ("VALE3", date(2024, 1, 31), "010", 20, 25, 18, 22, 200, *silver_metadata),
                ("VALE3", date(2024, 2, 5), "010", 30, 35, 28, 33, 300, *silver_metadata),
                ("VALE5", date(2024, 1, 2), "010", 10, 12, 9, 11, 100, *silver_metadata),
                ("PETR4", date(2024, 1, 2), "010", 10, 12, 9, 11, 100, *silver_metadata),
                ("VALE3", date(2024, 1, 3), "020", 10, 12, 9, 11, 100, *silver_metadata),
                ("VALE3", date(2024, 1, 4), "010", 10, 8, 9, 11, 100, *silver_metadata),
            ],
        )
        connection.execute(
            f"COPY silver TO '{_escape_sql_path(path)}' (FORMAT PARQUET)"
        )
    finally:
        connection.close()


def _escape_sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


if __name__ == "__main__":
    unittest.main()
