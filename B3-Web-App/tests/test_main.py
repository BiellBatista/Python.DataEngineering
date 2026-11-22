import tempfile
import unittest
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import duckdb
from fastapi import HTTPException
from pydantic import ValidationError

from backend import main


class DiscoverTablesTests(unittest.TestCase):
    def test_discovers_datasets_and_partitioned_files_by_layer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            data_lake = Path(temporary_directory)
            parquet_file = (
                data_lake
                / "bronze"
                / "cotacoes"
                / "year=2025"
                / "part-000.parquet"
            )
            parquet_file.parent.mkdir(parents=True)
            parquet_file.touch()
            silver_file = data_lake / "silver" / "cotacoes.parquet"
            silver_file.parent.mkdir(parents=True)
            silver_file.touch()
            gold_file = (
                data_lake
                / "gold"
                / "candles"
                / "ticker=VALE3"
                / "part-000.parquet"
            )
            gold_file.parent.mkdir(parents=True)
            gold_file.touch()

            with patch.object(main, "DATA_LAKE_ROOT", data_lake):
                tables = main._discover_tables()

        self.assertEqual(
            [(table["layer"], table["name"]) for table in tables],
            [
                ("bronze", "cotacoes"),
                ("silver", "data"),
                ("gold", "candles"),
            ],
        )
        self.assertEqual(tables[0]["qualified_name"], '"bronze"."cotacoes"')
        self.assertEqual(tables[2]["qualified_name"], '"gold"."candles"')

    def test_catalog_registers_gold_candles_and_hive_ticker_partition(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            data_lake = Path(temporary_directory)
            gold_partition = data_lake / "gold" / "candles" / "ticker=VALE3"
            gold_partition.mkdir(parents=True)
            gold_file = gold_partition / "part.parquet"

            source = duckdb.connect(":memory:")
            try:
                source.execute(
                    "COPY (SELECT DATE '2024-01-02' AS time, "
                    "'010' AS tipo_mercado, DECIMAL '10.00' AS open, "
                    "DECIMAL '12.00' AS high, DECIMAL '9.00' AS low, "
                    "DECIMAL '11.00' AS close, "
                    "DECIMAL '100.00' AS volume_total) "
                    f"TO '{gold_file}' (FORMAT PARQUET)"
                )
            finally:
                source.close()

            with patch.object(main, "DATA_LAKE_ROOT", data_lake):
                tables = main._discover_tables()

            connection = duckdb.connect(":memory:")
            try:
                main._register_tables(connection, tables)
                row = connection.execute(
                    'SELECT ticker, time, close FROM "gold"."candles"'
                ).fetchone()

                @contextmanager
                def ready_connection():
                    yield connection, tables

                with patch.object(main, "_ready_connection", ready_connection):
                    response = main.catalog()
            finally:
                connection.close()

        self.assertEqual(row, ("VALE3", date(2024, 1, 2), Decimal("11.00")))
        gold_layer = next(
            layer for layer in response["layers"] if layer["name"] == "gold"
        )
        self.assertEqual(
            gold_layer["tables"],
            [{"name": "candles", "qualified_name": '"gold"."candles"'}],
        )

    def test_quotes_dataset_names_for_sql_identifiers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            data_lake = Path(temporary_directory)
            parquet_file = (
                data_lake
                / "bronze"
                / 'daily-quotes "raw"'
                / "part.parquet"
            )
            parquet_file.parent.mkdir(parents=True)
            parquet_file.touch()

            with patch.object(main, "DATA_LAKE_ROOT", data_lake):
                tables = main._discover_tables()

        self.assertEqual(
            tables[0]["qualified_name"],
            '"bronze"."daily-quotes ""raw"""',
        )


class GoldCandlesApiTests(unittest.TestCase):
    def test_lists_only_valid_ticker_partitions_containing_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            for ticker in ("VALE3", "PETR4"):
                partition = root / f"ticker={ticker}"
                partition.mkdir()
                _write_gold_parquet(partition / "part.parquet")
            (root / "ticker=INVALID-1").mkdir()
            (root / "ticker=EMPTY3").mkdir()

            with patch.object(main, "GOLD_CANDLES_DIR", root):
                result = main.gold_candle_tickers()

        self.assertEqual(result.tickers, ["PETR4", "VALE3"])

    def test_reads_year_window_and_converts_decimal_values_to_floats(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            partition = root / "ticker=VALE3"
            partition.mkdir()
            _write_gold_parquet(partition / "part.parquet")
            with (
                patch.object(main, "GOLD_CANDLES_DIR", root),
                patch.object(main, "_connect_duckdb", side_effect=_connect_local),
            ):
                response = main.gold_candles(
                    "VALE3",
                    year=2025,
                    start_date=None,
                    end_date=None,
                    before_date=None,
                    limit=5000,
                )

        self.assertEqual(
            [candle.time for candle in response.candles],
            [date(2025, 1, 2), date(2025, 1, 3)],
        )
        self.assertIsInstance(response.candles[0].close, float)
        self.assertEqual(response.candles[0].close, 11.25)

    def test_before_date_returns_latest_prior_window_in_ascending_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            partition = root / "ticker=VALE3"
            partition.mkdir()
            _write_gold_parquet(partition / "part.parquet")
            with (
                patch.object(main, "GOLD_CANDLES_DIR", root),
                patch.object(main, "_connect_duckdb", side_effect=_connect_local),
            ):
                response = main.gold_candles(
                    "VALE3",
                    start_date=None,
                    end_date=None,
                    before_date=date(2025, 1, 4),
                    year=None,
                    limit=2,
                )

        self.assertEqual(
            [candle.time for candle in response.candles],
            [date(2025, 1, 2), date(2025, 1, 3)],
        )

    def test_filters_candles_by_timeframe(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            partition = root / "ticker=VALE3"
            partition.mkdir()
            _write_gold_parquet(partition / "part.parquet")
            with (
                patch.object(main, "GOLD_CANDLES_DIR", root),
                patch.object(main, "_connect_duckdb", side_effect=_connect_local),
            ):
                monthly = main.gold_candles(
                    "VALE3",
                    start_date=None,
                    end_date=None,
                    before_date=None,
                    year=None,
                    timeframe="1M",
                    limit=100,
                )
                with self.assertRaises(HTTPException) as invalid_timeframe:
                    main.gold_candles(
                        "VALE3",
                        start_date=None,
                        end_date=None,
                        before_date=None,
                        year=None,
                        timeframe="1h",
                        limit=100,
                    )

        self.assertEqual(
            [(candle.time, candle.timeframe) for candle in monthly.candles],
            [(date(2025, 1, 1), "1m")],
        )
        self.assertEqual(invalid_timeframe.exception.status_code, 422)

    def test_empty_partition_and_invalid_ticker_or_window(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "ticker=VALE3").mkdir()
            with patch.object(main, "GOLD_CANDLES_DIR", root):
                self.assertEqual(
                    main.gold_candles(
                        "VALE3",
                        start_date=None,
                        end_date=None,
                        before_date=None,
                        year=None,
                        limit=100,
                    ).candles,
                    [],
                )
                with self.assertRaises(HTTPException) as unsafe_ticker:
                    main.gold_candles(
                        "../etc",
                        start_date=None,
                        end_date=None,
                        before_date=None,
                        year=None,
                        limit=100,
                    )
                self.assertEqual(unsafe_ticker.exception.status_code, 422)

                with self.assertRaises(HTTPException) as invalid_range:
                    main.gold_candles(
                        "VALE3",
                        start_date=date(2025, 2, 1),
                        end_date=date(2025, 1, 1),
                        before_date=None,
                        year=None,
                        limit=100,
                    )
                self.assertEqual(invalid_range.exception.status_code, 422)

                with self.assertRaises(HTTPException) as conflicting_window:
                    main.gold_candles(
                        "VALE3",
                        start_date=None,
                        end_date=None,
                        before_date=date(2025, 1, 4),
                        year=2025,
                        limit=100,
                    )
                self.assertEqual(conflicting_window.exception.status_code, 422)


def _write_gold_parquet(path: Path) -> None:
    escaped_path = str(path).replace("'", "''")
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            f"""
            COPY (
                SELECT DATE '2024-12-31' AS time, '1d' AS timeframe,
                    '010' AS tipo_mercado,
                    DECIMAL '10.00' AS open, DECIMAL '12.00' AS high,
                    DECIMAL '9.00' AS low, DECIMAL '11.00' AS close,
                    DECIMAL '100.00' AS volume_total
                UNION ALL
                SELECT DATE '2025-01-02', '1d', '010', DECIMAL '10.25',
                    DECIMAL '12.50', DECIMAL '9.75', DECIMAL '11.25',
                    DECIMAL '200.00'
                UNION ALL
                SELECT DATE '2025-01-03', '1d', '010', DECIMAL '11.25',
                    DECIMAL '13.50', DECIMAL '10.75', DECIMAL '12.25',
                    DECIMAL '300.00'
                UNION ALL
                SELECT DATE '2025-01-01', '1m', '010', DECIMAL '10.25',
                    DECIMAL '13.50', DECIMAL '9.75', DECIMAL '12.25',
                    DECIMAL '500.00'
            ) TO '{escaped_path}' (FORMAT PARQUET)
            """
        )
    finally:
        connection.close()


def _connect_local() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")


class ValidateQueryTests(unittest.TestCase):
    def test_accepts_single_select_and_with_queries(self) -> None:
        main._validate_query("SELECT 1")
        main._validate_query("WITH values AS (SELECT 1) SELECT * FROM values")

    def test_rejects_non_select_and_multiple_statements(self) -> None:
        with self.assertRaises(main.QueryValidationError):
            main._validate_query("DELETE FROM bronze.cotacoes")
        with self.assertRaises(main.QueryValidationError):
            main._validate_query("SELECT 1; SELECT 2")

    def test_rejects_invalid_sql(self) -> None:
        with self.assertRaises(main.QueryValidationError):
            main._validate_query("SELECT FROM")


class FrontendLogValidationTests(unittest.TestCase):
    def test_accepts_known_event_and_safe_metadata(self) -> None:
        event = main.FrontendLogRequest(
            event="consulta_concluida",
            duration_ms=25,
            result_rows=10,
        )

        self.assertEqual(event.event, main.FrontendLogEvent.QUERY_COMPLETED)

    def test_rejects_unknown_events_and_arbitrary_query_text(self) -> None:
        with self.assertRaises(ValidationError):
            main.FrontendLogRequest(event="sql_completo")
        with self.assertRaises(ValidationError):
            main.FrontendLogRequest(
                event="consulta_concluida",
                sql="SELECT * FROM silver.cotacoes_historicas",
            )


if __name__ == "__main__":
    unittest.main()
