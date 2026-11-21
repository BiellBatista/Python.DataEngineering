"""Testes da configuração carregada do ambiente."""

import importlib
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import config


class ConfigTests(unittest.TestCase):
    def test_positive_integer_uses_default_and_rejects_invalid_values(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(config._positive_int_from_env("VALUE", 7), 7)

        with patch.dict(os.environ, {"VALUE": "12"}, clear=True):
            self.assertEqual(config._positive_int_from_env("VALUE", 7), 12)

        for value in ("invalid", "0", "-2"):
            with self.subTest(value=value), patch.dict(
                os.environ, {"VALUE": value}, clear=True
            ):
                with self.assertRaisesRegex(ValueError, "VALUE"):
                    config._positive_int_from_env("VALUE", 7)

    def test_import_reads_paths_and_numeric_settings_from_environment(self) -> None:
        environment = {
            "B3_DATA_LAKE_ROOT": "/tmp/test-lake",
            "B3_RAW_COTAHIST_DIR": "/tmp/raw",
            "B3_BRONZE_COTAHIST_DIR": "/tmp/bronze",
            "B3_SILVER_COTAHIST_DIR": "/tmp/silver",
            "B3_GOLD_CANDLES_DIR": "/tmp/gold",
            "DUCKDB_THREADS": "3",
            "DUCKDB_PORT": "9495",
            "BRONZE_MAX_WORKERS": "2",
            "B3_START_YEAR": "2023",
            "B3_END_YEAR": "2024",
        }
        with patch.dict(os.environ, environment, clear=True):
            try:
                importlib.reload(config)
                self.assertEqual(config.DATA_LAKE_ROOT, Path("/tmp/test-lake"))
                self.assertEqual(config.RAW_COTAHIST_DIR, "/tmp/raw")
                self.assertEqual(config.BRONZE_COTAHIST_DIR, "/tmp/bronze")
                self.assertEqual(config.SILVER_COTAHIST_DIR, "/tmp/silver")
                self.assertEqual(config.GOLD_CANDLES_DIR, "/tmp/gold")
                self.assertEqual(config.DUCKDB_THREADS, 3)
                self.assertEqual(config.DUCKDB_PORT, 9495)
                self.assertEqual(config.BRONZE_MAX_WORKERS, 2)
                self.assertEqual((config.DEFAULT_START_YEAR, config.DEFAULT_END_YEAR), (2023, 2024))
            finally:
                importlib.reload(config)

    def test_import_rejects_reversed_year_range(self) -> None:
        with patch.dict(
            os.environ,
            {"B3_START_YEAR": "2025", "B3_END_YEAR": "2024"},
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "B3_START_YEAR"):
                importlib.reload(config)
        importlib.reload(config)

    def test_import_rejects_invalid_integer_setting(self) -> None:
        with patch.dict(os.environ, {"DUCKDB_PORT": "invalid"}, clear=True):
            with self.assertRaisesRegex(ValueError, "DUCKDB_PORT"):
                importlib.reload(config)
        importlib.reload(config)


if __name__ == "__main__":
    unittest.main()
