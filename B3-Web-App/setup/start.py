from __future__ import annotations

import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

from env_file import read_env


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"
STARTUP_TIMEOUT_SECONDS = 300


def run_compose(*arguments: str) -> None:
    command = ["docker", "compose", *arguments]
    print(f"$ {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def wait_for_application(url: str) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    last_error = "o endpoint ainda não respondeu"

    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=5) as response:
                if response.status == 200:
                    return
                last_error = f"HTTP {response.status}"
        except (OSError, URLError) as error:
            last_error = str(error)
        time.sleep(2)

    raise RuntimeError(
        f"A aplicação não ficou disponível em {url} após "
        f"{STARTUP_TIMEOUT_SECONDS} segundos ({last_error}). "
        "Consulte `docker compose ps` e `docker compose logs --tail=100 web`."
    )


def validate_environment(values: dict[str, str]) -> tuple[str, str]:
    required = ("B3_DATA_LAKE_ROOT", "QUACK_TOKEN")
    missing = [
        name
        for name in required
        if not values.get(name) or values[name].startswith("replace-with-")
    ]
    if missing:
        raise ValueError(
            "Configure estas variáveis em .env antes de iniciar: "
            + ", ".join(missing)
        )

    data_lake_root = Path(values["B3_DATA_LAKE_ROOT"]).expanduser()
    if not data_lake_root.is_dir():
        raise ValueError(
            f"B3_DATA_LAKE_ROOT não existe ou não é um diretório: {data_lake_root}"
        )

    network_name = values.get(
        "B3_DOCKER_NETWORK",
        "b3-pipelines_airflow_network",
    )
    network = subprocess.run(
        ["docker", "network", "inspect", network_name],
        capture_output=True,
        text=True,
        check=False,
    )
    if network.returncode != 0:
        raise ValueError(
            f"A rede Docker {network_name!r} não existe. Inicie primeiro "
            "B3-Pipelines com ./start.sh e confirme B3_DOCKER_NETWORK."
        )

    web_port = values.get("WEB_PORT", "4200")
    if not web_port.isdigit() or not 1 <= int(web_port) <= 65535:
        raise ValueError("WEB_PORT deve estar entre 1 e 65535.")
    return network_name, web_port


def main() -> int:
    if not ENV_FILE.is_file():
        print("Execute ./setup.sh e configure .env antes de iniciar.", file=sys.stderr)
        return 1
    if shutil.which("docker") is None:
        print("Docker não encontrado. Instale Docker Compose v2.", file=sys.stderr)
        return 1

    try:
        values = read_env(ENV_FILE)
        _, web_port = validate_environment(values)
        run_compose("config", "--quiet")
        run_compose("up", "--build", "-d")
        app_url = f"http://localhost:{web_port}"
        print("Aguardando a API e o DuckDBServer...", flush=True)
        wait_for_application(f"{app_url}/api/health")
        print(f"Data Process disponível em {app_url}", flush=True)
        return 0
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Erro ao iniciar o ambiente: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
