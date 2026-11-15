#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
    echo "Python 3 não encontrado. Instale Python 3 para iniciar o ambiente." >&2
    exit 1
fi

exec python3 "${PROJECT_ROOT}/setup/start.py"
