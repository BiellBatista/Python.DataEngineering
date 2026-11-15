"""Testes do ponto de entrada do pipeline."""

import re
import runpy
import unittest
from pathlib import Path
from unittest.mock import patch

import main
from historical_series.transformations import bronze, silver


class MainTests(unittest.TestCase):
    def test_main_runs_bronze_then_silver_with_one_run_id(self) -> None:
        with (
            patch("main.execute_bronze_load") as bronze_load,
            patch("main.execute_silver_load") as silver_load,
        ):
            main.main()

        bronze_args, bronze_kwargs = bronze_load.call_args
        silver_args, silver_kwargs = silver_load.call_args
        self.assertEqual(bronze_args[0], silver_args[0])
        self.assertRegex(bronze_args[0], r"^manual-\d{8}T\d{6}Z$")
        self.assertEqual(bronze_args[1:3], silver_args[1:3])
        self.assertEqual(bronze_kwargs["ingestion_time"].utcoffset().total_seconds(), 0)
        self.assertEqual(bronze_kwargs["threads"], silver_kwargs["threads"])
        self.assertEqual(bronze_kwargs["memory_limit"], silver_kwargs["memory_limit"])

    def test_module_entry_point_invokes_main(self) -> None:
        source = Path(main.__file__)
        with (
            patch.object(bronze, "execute_bronze_load") as bronze_load,
            patch.object(silver, "execute_silver_load") as silver_load,
        ):
            runpy.run_path(str(source), run_name="__main__")

        bronze_load.assert_called_once()
        silver_load.assert_called_once()


if __name__ == "__main__":
    unittest.main()
