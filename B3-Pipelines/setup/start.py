from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STARTUP_TIMEOUT_SECONDS = 300
AIRFLOW_DAG_ID = "b3_cotahist_medallion_pipeline"


def configured_airflow_port() -> int:
    port = os.environ.get("AIRFLOW_PORT")
    if port is None:
        env_file = PROJECT_ROOT / ".env"
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition("=")
            if separator and key.strip() == "AIRFLOW_PORT":
                port = value.strip().strip("\"'")
                break

    if not port:
        return 8080

    try:
        parsed_port = int(port)
    except ValueError as error:
        raise ValueError("AIRFLOW_PORT deve ser um número entre 1 e 65535.") from error

    if not 1 <= parsed_port <= 65535:
        raise ValueError("AIRFLOW_PORT deve ser um número entre 1 e 65535.")
    return parsed_port


def wait_for_airflow(url: str) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    last_error = "o endpoint ainda não respondeu"

    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=5) as response:
                if response.status == 200:
                    return
                last_error = f"Airflow respondeu com HTTP {response.status}"
        except OSError as error:
            last_error = str(error)

        time.sleep(2)

    raise RuntimeError(
        f"Airflow não ficou disponível em {url} após "
        f"{STARTUP_TIMEOUT_SECONDS} segundos ({last_error}). "
        "Confira os serviços com `docker compose ps` e "
        "`docker compose logs --tail=100`."
    )


def run_compose(*arguments: str) -> None:
    command = ["docker", "compose", *arguments]
    print(f"$ {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def main() -> int:
    env_file = PROJECT_ROOT / ".env"
    if not env_file.is_file():
        print(
            "Arquivo .env não encontrado. Copie .env.example para .env, "
            "configure as credenciais e B3_DATA_LAKE_ROOT e tente novamente.",
            file=sys.stderr,
        )
        return 1

    if shutil.which("docker") is None:
        print("Docker não encontrado. Instale o Docker com Docker Compose v2.", file=sys.stderr)
        return 1

    try:
        port = configured_airflow_port()
        run_compose("config", "--quiet")
        run_compose("up", "--build", "-d")

        airflow_url = f"http://localhost:{port}"
        health_url = f"{airflow_url}/api/v2/monitor/health"
        print("Aguardando o Airflow ficar saudável...", flush=True)
        wait_for_airflow(health_url)

        dag_url = f"{airflow_url}/dags/{AIRFLOW_DAG_ID}"
        print(f"Airflow disponível. Abrindo a pipeline: {dag_url}", flush=True)
        import webbrowser

        if not webbrowser.open(dag_url, new=2):
            print("Não foi possível abrir o navegador automaticamente.")
            print(f"Acesse manualmente: {dag_url}")
        return 0
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Erro ao iniciar o ambiente: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
