#!/bin/bash
PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${PROJECT_ROOT}/venv/bin/activate"
export PYTHONPATH="${PROJECT_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"
export AIRFLOW_HOME="${PROJECT_ROOT}/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW_HOME}/dags"
exec airflow standalone
