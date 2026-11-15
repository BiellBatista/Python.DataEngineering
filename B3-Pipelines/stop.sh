#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v docker >/dev/null 2>&1; then
    echo "Docker não encontrado. Instale o Docker com Docker Compose v2." >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
exec docker compose down
