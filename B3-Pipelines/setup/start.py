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


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() and not key.lstrip().startswith("#"):
            values[key.strip()] = value.strip().strip("\"'")
    return values


def validate_environment(env_file: Path) -> None:
    values = read_env(env_file)
    required = (
        "B3_DATA_LAKE_ROOT",
        "AIRFLOW_DB_PASSWORD",
        "AIRFLOW_ADMIN_PASSWORD",
        "GRAFANA_ADMIN_PASSWORD",
        "QUACK_TOKEN",
        "AIRFLOW__API_AUTH__JWT_SECRET",
    )
    configured = {
        name: os.environ.get(name, values.get(name, ""))
        for name in required
    }
    missing = [
        name
        for name in required
        if not configured[name] or configured[name].startswith("replace-with-")
    ]
    if missing:
        raise ValueError(
            "Configure estas variáveis em .env antes de iniciar: "
            + ", ".join(missing)
        )

    data_lake_root = Path(configured["B3_DATA_LAKE_ROOT"]).expanduser()
    if not data_lake_root.is_dir():
        raise ValueError(
            f"B3_DATA_LAKE_ROOT não existe ou não é um diretório: {data_lake_root}"
        )


def configured_port(environment_variable: str, default: int) -> int:
    port = os.environ.get(environment_variable)
    if port is None:
        env_file = PROJECT_ROOT / ".env"
        if env_file.is_file():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                key, separator, value = line.partition("=")
                if separator and key.strip() == environment_variable:
                    port = value.strip().strip("\"'")
                    break

    if not port:
        return default

    try:
        parsed_port = int(port)
    except ValueError as error:
        raise ValueError(
            f"{environment_variable} deve ser um número entre 1 e 65535."
        ) from error

    if not 1 <= parsed_port <= 65535:
        raise ValueError(f"{environment_variable} deve ser um número entre 1 e 65535.")
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
        try:
            with env_file.open("x", encoding="utf-8") as destination:
                destination.write(
                    (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
                )
            os.chmod(env_file, 0o600)
            print(f"Arquivo de ambiente criado: {env_file}")
        except FileExistsError:
            pass
        except OSError as error:
            print(f"Não foi possível criar o arquivo .env: {error}", file=sys.stderr)
            return 1

    try:
        validate_environment(env_file)
        if shutil.which("docker") is None:
            print("Docker não encontrado. Instale o Docker com Docker Compose v2.", file=sys.stderr)
            return 1

        airflow_port = configured_port("AIRFLOW_PORT", 8080)
        grafana_port = configured_port("GRAFANA_PORT", 3000)
        run_compose("config", "--quiet")
        run_compose("up", "--build", "-d")

        airflow_url = f"http://localhost:{airflow_port}"
        health_url = f"{airflow_url}/api/v2/monitor/health"
        print("Aguardando o Airflow ficar saudável...", flush=True)
        wait_for_airflow(health_url)

        dag_url = f"{airflow_url}/dags/{AIRFLOW_DAG_ID}"
        grafana_url = f"http://localhost:{grafana_port}"
        print("Ambiente disponível. Acesse as ferramentas:", flush=True)
        print(f"  Airflow: {airflow_url}", flush=True)
        print(f"  Pipeline COTAHIST: {dag_url}", flush=True)
        print(f"  Grafana (métricas, logs e traces): {grafana_url}", flush=True)
        print(f"  Grafana Explore: {grafana_url}/explore", flush=True)
        return 0
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Erro ao iniciar o ambiente: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
