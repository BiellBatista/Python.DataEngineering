#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
    echo "Python 3 não encontrado. Instale Python 3 para iniciar o launcher." >&2
    exit 1
fi

python3 "${PROJECT_ROOT}/setup/setup.py"

VENV_PATH="${PROJECT_ROOT}/backend/venv"
VENV_PYTHON="${VENV_PATH}/bin/python"

if [[ ! -x "${VENV_PYTHON}" ]]; then
    python3 -m venv "${VENV_PATH}"
    "${VENV_PYTHON}" -m pip install "${PROJECT_ROOT}/backend"
fi

if [[ ! -d "${PROJECT_ROOT}/frontend/node_modules" ]]; then
    if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
        echo "Node.js e npm são necessários para instalar as dependências locais do frontend." >&2
        exit 1
    fi
    npm --prefix "${PROJECT_ROOT}/frontend" ci
fi

exec "${VENV_PYTHON}" "${PROJECT_ROOT}/setup/start.py"
