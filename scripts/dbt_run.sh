#!/usr/bin/env bash
set -euo pipefail
cd /opt/dbt
export DBT_PROFILES_DIR=${DBT_PROFILES_DIR:-/opt/dbt/profiles}

dbt --version
dbt debug
dbt run
dbt test
