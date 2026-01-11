import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Astro paths:
# - dags:     /usr/local/airflow/dags
# - include:  /usr/local/airflow/include
INCLUDE = "/usr/local/airflow/include"

RAW_DIR = f"{INCLUDE}/data/raw"
DBT_DIR = f"{INCLUDE}/dbt_supplychain"
DBT_PROFILES_DIR = f"{DBT_DIR}/profiles"

BQ_LOADER = f"{INCLUDE}/scripts/bq_load_raw.py"
KG_EXPORTER = f"{INCLUDE}/kg/export/export_supplychain_kg.py"
KG_LOADER = f"{INCLUDE}/kg/load/load_fuseki.py"

TTL_OUT = os.environ.get("TTL_OUT", f"{INCLUDE}/data/kg/supplychain.ttl")
DBT_BIN = os.environ.get("DBT_BIN", "/usr/local/airflow/dbt_venv/bin/dbt")

# Fuseki (inside docker network)
FUSEKI_URL = os.environ.get("FUSEKI_URL", "http://fuseki:3030")
FUSEKI_DATASET = os.environ.get("FUSEKI_DATASET", "sc")


def _run_and_log(cmd: list[str]) -> None:
    """Run a subprocess and always print stdout/stderr into Airflow logs."""
    import subprocess

    p = subprocess.run(cmd, text=True, capture_output=True)
    if p.stdout:
        print(p.stdout)
    if p.stderr:
        print(p.stderr)
    p.check_returncode()


def _bq_load_raw():
    _run_and_log(["python", "-u", BQ_LOADER, "--raw-dir", RAW_DIR])


def _export_rdf():
    _run_and_log(["python", "-u", KG_EXPORTER, "--out", TTL_OUT])


def _load_fuseki():
    import subprocess, os
    ttl_out = os.environ.get("TTL_OUT", "/usr/local/airflow/include/data/kg/supplychain.ttl")
    fuseki_url = os.environ.get("FUSEKI_URL", "http://host.docker.internal:3030")
    dataset = os.environ.get("FUSEKI_DATASET", "sc")

    subprocess.check_call([
        "python", "-u", "/usr/local/airflow/include/kg/load/load_fuseki.py",
        "--fuseki-url", fuseki_url,
        "--dataset", dataset,
        "--ttl", ttl_out
    ])



with DAG(
    dag_id="supplychain_kg_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual trigger
    catchup=False,
    default_args={"retries": 0},
    tags=["supplychain", "dbt", "kg", "rdf", "fuseki"],
) as dag:

    load_raw = PythonOperator(
        task_id="load_raw_to_bigquery",
        python_callable=_bq_load_raw,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_and_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && "
            f"{DBT_BIN} --version && "
            f"{DBT_BIN} debug && "
            f"{DBT_BIN} run && "
            f"{DBT_BIN} test"
        ),
    )

    export_rdf = PythonOperator(
        task_id="export_rdf_ttl",
        python_callable=_export_rdf,
    )

    load_graph = PythonOperator(
        task_id="load_ttl_to_fuseki",
        python_callable=_load_fuseki,
    )

    load_raw >> dbt_run >> export_rdf >> load_graph
