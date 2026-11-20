"""Testes do launcher da stack Docker."""

import importlib.util
import os
import unittest
import webbrowser
from contextlib import redirect_stdout
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
            (project_root / ".env").touch()
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


if __name__ == "__main__":
    unittest.main()
