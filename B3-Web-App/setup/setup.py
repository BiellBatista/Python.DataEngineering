from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from env_file import read_env


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"
PIPELINES_ENV = PROJECT_ROOT.parent / "B3-Pipelines" / ".env"
SHARED_VARIABLES = ("B3_DATA_LAKE_ROOT", "B3_DOCKER_NETWORK", "QUACK_TOKEN")


def initialize_environment() -> None:
    if ENV_FILE.exists():
        print(f"Preservando a configuração existente: {ENV_FILE}")
        return

    content = ENV_EXAMPLE.read_text(encoding="utf-8")
    pipelines_values = read_env(PIPELINES_ENV)
    for name in SHARED_VARIABLES:
        value = pipelines_values.get(name, "")
        if value and not value.startswith("replace-with-"):
            content = re.sub(
                rf"(?m)^{re.escape(name)}=.*$",
                f"{name}={value}",
                content,
            )

    try:
        descriptor = os.open(
            ENV_FILE,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
    except FileExistsError:
        print(f"Preservando a configuração existente: {ENV_FILE}")
        return

    with os.fdopen(descriptor, "w", encoding="utf-8") as env_file:
        env_file.write(content)
    print(f"Arquivo de ambiente criado: {ENV_FILE}")
    if pipelines_values:
        print("Valores existentes do B3-Pipelines foram reutilizados quando disponíveis.")
    print("Revise B3_DATA_LAKE_ROOT e QUACK_TOKEN antes de executar ./start.sh.")


def main() -> int:
    try:
        initialize_environment()
    except OSError as error:
        print(f"Não foi possível preparar o ambiente: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
