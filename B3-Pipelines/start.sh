#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
    echo "Python 3 não encontrado. Instale Python 3 para iniciar o ambiente." >&2
    exit 1
fi

if [[ ! -f "${PROJECT_ROOT}/.env" ]]; then
    (umask 077; cp "${PROJECT_ROOT}/.env.example" "${PROJECT_ROOT}/.env")
    echo "Arquivo .env criado a partir de .env.example."
fi

VENV_PATH="${PROJECT_ROOT}/venv"
VENV_PYTHON="${VENV_PATH}/bin/python"

if [[ ! -x "${VENV_PYTHON}" ]]; then
    python3 -m venv "${VENV_PATH}"
    "${VENV_PYTHON}" -m pip install -e "${PROJECT_ROOT}[test]"
fi

exec "${VENV_PYTHON}" "${PROJECT_ROOT}/setup/start.py"
