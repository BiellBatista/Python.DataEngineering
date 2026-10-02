#!/bin/bash
source .venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow standalone
