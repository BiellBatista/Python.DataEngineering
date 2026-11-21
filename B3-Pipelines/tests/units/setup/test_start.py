"""Testes do launcher da stack Docker."""

import importlib.util
import os
import unittest
import webbrowser
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[3]
START_SCRIPT = PROJECT_ROOT / "setup" / "start.py"
SPEC = importlib.util.spec_from_file_location("start_script", START_SCRIPT)
assert SPEC is not None and SPEC.loader is not None
start_script = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(start_script)


class StartTests(unittest.TestCase):
    def test_main_prints_tool_urls_without_opening_browser(self) -> None:
        with TemporaryDirectory() as directory:
            project_root = Path(directory)
            data_lake_root = project_root / "data-lake"
            data_lake_root.mkdir()
            (project_root / ".env").write_text(
                "\n".join(
                    (
                        f"B3_DATA_LAKE_ROOT={data_lake_root}",
                        "AIRFLOW_DB_PASSWORD=db-password",
                        "AIRFLOW_ADMIN_PASSWORD=admin-password",
                        "GRAFANA_ADMIN_PASSWORD=grafana-password",
                        "QUACK_TOKEN=quack-token",
                        "AIRFLOW__API_AUTH__JWT_SECRET=jwt-secret",
                    )
                ),
                encoding="utf-8",
            )
            output = StringIO()
            with (
                patch.object(start_script, "PROJECT_ROOT", project_root),
                patch.object(start_script.shutil, "which", return_value="/usr/bin/docker"),
                patch.object(start_script, "run_compose") as run_compose,
                patch.object(start_script, "wait_for_airflow"),
                patch.object(webbrowser, "open") as open_browser,
                patch.dict(
                    os.environ,
                    {"AIRFLOW_PORT": "8088", "GRAFANA_PORT": "3001"},
                ),
                redirect_stdout(output),
            ):
                result = start_script.main()

        self.assertEqual(result, 0)
        self.assertIn("http://localhost:8088", output.getvalue())
        self.assertIn(
            "http://localhost:8088/dags/b3_cotahist_medallion_pipeline",
            output.getvalue(),
        )
        self.assertIn("http://localhost:3001", output.getvalue())
        self.assertIn("http://localhost:3001/explore", output.getvalue())
        run_compose.assert_any_call("config", "--quiet")
        run_compose.assert_any_call("up", "--build", "-d")
        open_browser.assert_not_called()

    def test_main_creates_env_example_and_rejects_placeholder_credentials(self) -> None:
        with TemporaryDirectory() as directory:
            project_root = Path(directory)
            (project_root / ".env.example").write_text(
                "QUACK_TOKEN=replace-with-a-long-random-token\n",
                encoding="utf-8",
            )
            output = StringIO()
            errors = StringIO()
            with (
                patch.object(start_script, "PROJECT_ROOT", project_root),
                patch.object(start_script.shutil, "which", return_value="/usr/bin/docker"),
                redirect_stdout(output),
                redirect_stderr(errors),
            ):
                result = start_script.main()

            env_file = project_root / ".env"
            self.assertTrue(env_file.is_file())
            self.assertEqual(env_file.stat().st_mode & 0o777, 0o600)

        self.assertEqual(result, 1)
        self.assertIn("Arquivo de ambiente criado", output.getvalue())
        self.assertIn("QUACK_TOKEN", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
