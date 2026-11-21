import importlib.util
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


SETUP_SCRIPT = Path(__file__).resolve().parents[1] / "setup" / "setup.py"
SPEC = importlib.util.spec_from_file_location("web_setup_script", SETUP_SCRIPT)
assert SPEC is not None and SPEC.loader is not None
setup_script = importlib.util.module_from_spec(SPEC)
sys.path.insert(0, str(SETUP_SCRIPT.parent))
try:
    SPEC.loader.exec_module(setup_script)
finally:
    sys.path.remove(str(SETUP_SCRIPT.parent))


class EnvironmentSetupTests(unittest.TestCase):
    def test_creates_restricted_env_and_reuses_pipeline_values(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            env_file = root / ".env"
            env_example = root / ".env.example"
            pipelines_env = root / "pipelines.env"
            env_example.write_text(
                "B3_DATA_LAKE_ROOT=example-path\n"
                "B3_DOCKER_NETWORK=example-network\n"
                "QUACK_TOKEN=replace-with-a-long-random-token\n",
                encoding="utf-8",
            )
            pipelines_env.write_text(
                "B3_DATA_LAKE_ROOT=/data/lake\n"
                "B3_DOCKER_NETWORK=shared-network\n"
                "QUACK_TOKEN=shared-token\n",
                encoding="utf-8",
            )

            with (
                patch.object(setup_script, "ENV_FILE", env_file),
                patch.object(setup_script, "ENV_EXAMPLE", env_example),
                patch.object(setup_script, "PIPELINES_ENV", pipelines_env),
            ):
                setup_script.initialize_environment()

            content = env_file.read_text(encoding="utf-8")
            self.assertIn("B3_DATA_LAKE_ROOT=/data/lake", content)
            self.assertIn("B3_DOCKER_NETWORK=shared-network", content)
            self.assertIn("QUACK_TOKEN=shared-token", content)
            self.assertEqual(env_file.stat().st_mode & 0o777, 0o600)

    def test_preserves_an_existing_env_file(self) -> None:
        with TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text("KEEP=this-value\n", encoding="utf-8")

            with patch.object(setup_script, "ENV_FILE", env_file):
                setup_script.initialize_environment()

            self.assertEqual(env_file.read_text(encoding="utf-8"), "KEEP=this-value\n")


if __name__ == "__main__":
    unittest.main()
