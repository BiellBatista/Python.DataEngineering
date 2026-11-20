"""Testes de contexto de parâmetros nos logs."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from common.exceptions import BronzeError
from common.log_context import log_parameters
from historical_series.data_qualities import data_quality
from historical_series.transformations import bronze, silver
from structlog.contextvars import get_contextvars


class LogParametersTests(unittest.TestCase):
    def test_binds_arguments_and_defaults_for_the_function_scope(self) -> None:
        @log_parameters
        def operation(start_year: int, input_dir: str = "/raw") -> dict[str, object]:
            return get_contextvars() | {
                "function_args": (start_year, input_dir),
            }

        context = operation(2024)

        self.assertEqual(context["start_year"], 2024)
        self.assertEqual(context["input_dir"], "/raw")
        self.assertEqual(context["function_args"], (2024, "/raw"))
        self.assertNotIn("start_year", get_contextvars())

    def test_restores_context_after_function_failure(self) -> None:
        @log_parameters
        def operation(start_year: int) -> None:
            raise RuntimeError(f"expected failure: {start_year}")

        with self.assertRaisesRegex(RuntimeError, "expected failure"):
            operation(2024)

        self.assertNotIn("start_year", get_contextvars())

    def test_data_quality_methods_bind_their_arguments(self) -> None:
        captured_contexts: list[dict[str, object]] = []

        def capture_context(*args: object) -> None:
            self.assertTrue(args)
            captured_contexts.append(get_contextvars())

        with (
            patch.object(
                data_quality,
                "_validate_raw_quality",
                side_effect=capture_context,
            ),
            patch.object(
                data_quality,
                "_validate_bronze_quality",
                side_effect=capture_context,
            ),
        ):
            data_quality.validate_raw_quality(2023, 2024, "/raw")
            data_quality.validate_bronze_quality(
                2023, 2024, "/bronze", "4GB", 2
            )

        self.assertEqual(
            captured_contexts,
            [
                {"start_year": 2023, "end_year": 2024, "input_dir": "/raw"},
                {
                    "start_year": 2023,
                    "end_year": 2024,
                    "input_dir": "/bronze",
                    "memory_limit": "4GB",
                    "threads": 2,
                },
            ],
        )

    def test_bronze_worker_threads_inherit_load_parameters(self) -> None:
        captured_contexts: list[dict[str, object]] = []

        def capture_worker_context(*args: object) -> None:
            self.assertEqual(len(args), 6)
            captured_contexts.append(get_contextvars())

        with (
            patch.object(bronze, "list_raw_dirs", return_value=["raw.txt"]),
            patch.object(
                bronze,
                "_load_raw_dir",
                side_effect=capture_worker_context,
            ),
            tempfile.TemporaryDirectory() as temporary_directory,
        ):
            bronze.execute_bronze_load(
                dag_run_id="run-2024",
                start_year=2024,
                end_year=2024,
                memory_limit="4GB",
                threads=2,
                max_workers=1,
                input_dir="/raw",
                output_dir=str(Path(temporary_directory) / "bronze"),
            )

        self.assertEqual(len(captured_contexts), 1)
        self.assertEqual(
            captured_contexts[0]["dag_run_id"],
            "run-2024",
        )
        self.assertEqual(captured_contexts[0]["start_year"], 2024)
        self.assertEqual(captured_contexts[0]["input_dir"], "/raw")
        self.assertEqual(
            captured_contexts[0]["output_dir"],
            str(Path(temporary_directory) / "bronze"),
        )

    def test_silver_logs_are_emitted_with_load_parameters(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "bronze" / "year=2024").mkdir(parents=True)
            captured_contexts: list[dict[str, object]] = []

            def capture_log_context(
                message: str,
                *args: object,
                **kwargs: object,
            ) -> None:
                captured_contexts.append(
                    get_contextvars() | {"log_call": (message, args, kwargs)}
                )

            with (
                patch.object(
                    silver.logger,
                    "info",
                    side_effect=capture_log_context,
                ),
                patch.object(silver, "find_most_recent_bronze_parquets", return_value=[]),
            ):
                with self.assertRaises(BronzeError):
                    silver.execute_silver_load(
                        dag_run_id="run-2024",
                        start_year=2024,
                        end_year=2024,
                        memory_limit="4GB",
                        threads=2,
                        input_dir=str(root / "bronze"),
                        output_dir=str(root / "silver"),
                    )

        self.assertTrue(captured_contexts)
        self.assertEqual(captured_contexts[0]["dag_run_id"], "run-2024")
        self.assertEqual(captured_contexts[0]["start_year"], 2024)
        self.assertEqual(captured_contexts[0]["input_dir"], str(root / "bronze"))


if __name__ == "__main__":
    unittest.main()
