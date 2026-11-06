#!/bin/bash
PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ -x "${PROJECT_ROOT}/.venv/bin/airflow" ]]; then
    VENV_PATH="${PROJECT_ROOT}/.venv"
elif [[ -x "${PROJECT_ROOT}/venv/bin/airflow" ]]; then
    VENV_PATH="${PROJECT_ROOT}/venv"
else
    echo "Airflow não encontrado em .venv ou venv. Instale o extra airflow." >&2
    exit 1
fi

source "${VENV_PATH}/bin/activate"
export PYTHONPATH="${PROJECT_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"
export AIRFLOW_HOME="${PROJECT_ROOT}/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW_HOME}/dags"
exec airflow standalone
