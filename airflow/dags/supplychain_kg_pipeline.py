import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _bq_load_raw():
    import subprocess
    subprocess.check_call(["python", "/opt/scripts/bq_load_raw.py", "--raw-dir", "/opt/project/data/raw"])


def _export_rdf():
    import subprocess
    ttl_out = os.environ.get("TTL_OUT", "/opt/project/data/kg/supplychain.ttl")
    subprocess.check_call(["python", "/opt/kg/export/export_supplychain_kg.py", "--out", ttl_out])


def _load_fuseki():
    import subprocess
    ttl_out = os.environ.get("TTL_OUT", "/opt/project/data/kg/supplychain.ttl")
    subprocess.check_call(["python", "/opt/kg/load/load_fuseki.py", "--ttl", ttl_out])


with DAG(
    dag_id="supplychain_kg_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
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
        bash_command="bash /opt/scripts/dbt_run.sh",
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
