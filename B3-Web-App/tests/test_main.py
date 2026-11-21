import tempfile
import unittest
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import duckdb
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
