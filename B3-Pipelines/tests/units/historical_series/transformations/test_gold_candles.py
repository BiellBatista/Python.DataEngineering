"""Testes da transformação de candles da camada Gold."""

import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

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
                    "SELECT ticker, time, tipo_mercado, open, high, low, close, "
                    "volume_total FROM read_parquet(?, hive_partitioning = true) "
                    "ORDER BY time",
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

            self.assertEqual(
                rows,
                [
                    (
                        "VALE3",
                        date(2024, 1, 2),
                        "010",
                        Decimal("10.00"),
                        Decimal("12.00"),
                        Decimal("9.00"),
                        Decimal("11.00"),
                        Decimal("100.00"),
                    )
                ],
            )
            self.assertEqual(
                {name: types[name] for name in ("open", "high", "low", "close")},
                {name: "DECIMAL(18,2)" for name in ("open", "high", "low", "close")},
            )

            petr_partition = gold_root / "ticker=PETR4"
            petr_partition.mkdir()
            (petr_partition / "preserved").touch()
            execute_gold_candles_load(
                tickers=["VALE3", "VALE3"],
                memory_limit=None,
                threads=1,
                input_dir=str(silver_root),
                output_dir=str(gold_root),
            )
            self.assertTrue((petr_partition / "preserved").is_file())
            self.assertTrue(vale5_partition.is_dir())
            self.assertTrue(next(vale3_partition.glob("*.parquet")).is_file())

    def test_load_supports_suffix_and_defaults_to_all_tickers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver_root = root / "silver"
            silver_root.mkdir()
            _write_silver(silver_root / "part.parquet")
            suffix_output = root / "gold-suffix"
            execute_gold_candles_load(
                end_with="4",
                memory_limit=None,
                threads=1,
                input_dir=str(silver_root),
                output_dir=str(suffix_output),
            )
            self.assertTrue((suffix_output / "ticker=PETR4").is_dir())
            self.assertFalse((suffix_output / "ticker=VALE3").exists())

            all_output = root / "gold-all"
            execute_gold_candles_load(
                tickers=[],
                memory_limit=None,
                threads=1,
                input_dir=str(silver_root),
                output_dir=str(all_output),
            )
            self.assertTrue((all_output / "ticker=VALE3").is_dir())
            self.assertTrue((all_output / "ticker=VALE5").is_dir())
            self.assertTrue((all_output / "ticker=PETR4").is_dir())

    def test_load_rejects_conflicting_selectors_and_empty_results(self) -> None:
        with self.assertRaisesRegex(ValueError, "somente um seletor"):
            execute_gold_candles_load(
                tickers=["VALE3"],
                start_with="VALE",
                input_dir="/unused",
                output_dir="/unused",
            )

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver_root = root / "silver"
            silver_root.mkdir()
            silver_file = silver_root / "part.parquet"
            _write_silver(silver_file)
            with self.assertRaisesRegex(GoldError, "Nenhum ticker selecionado"):
                execute_gold_candles_load(
                    tickers=["UNKNOWN"],
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
            ({"tickers": "VALE3"}, "lista"),
            ({"tickers": [" "]}, "não vazio"),
            ({"start_with": " "}, "start_with"),
            ({"end_with": " "}, "end_with"),
            ({"threads": 0}, "threads"),
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
                volume_total DECIMAL(18, 2)
            )
            """
        )
        connection.executemany(
            "INSERT INTO silver VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("VALE3", date(2024, 1, 2), "010", 10, 12, 9, 11, 100),
                ("VALE5", date(2024, 1, 2), "010", 10, 12, 9, 11, 100),
                ("PETR4", date(2024, 1, 2), "010", 10, 12, 9, 11, 100),
                ("VALE3", date(2024, 1, 3), "020", 10, 12, 9, 11, 100),
                ("VALE3", date(2024, 1, 4), "010", 10, 8, 9, 11, 100),
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
