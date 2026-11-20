import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

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

            with patch.object(main, "DATA_LAKE_ROOT", data_lake):
                tables = main._discover_tables()

        self.assertEqual(
            [(table["layer"], table["name"]) for table in tables],
            [("bronze", "cotacoes"), ("silver", "data")],
        )
        self.assertEqual(tables[0]["qualified_name"], '"bronze"."cotacoes"')

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


if __name__ == "__main__":
    unittest.main()
