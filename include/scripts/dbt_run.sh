#!/usr/bin/env bash
set -euo pipefail

DBT_DIR="/usr/local/airflow/include/dbt_supplychain"
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-${DBT_DIR}/profiles}"
DBT_BIN="${DBT_BIN:-/usr/local/airflow/dbt_venv/bin/dbt}"

cd "$DBT_DIR"

$DBT_BIN --version
$DBT_BIN debug
$DBT_BIN run
$DBT_BIN test
