# Astro Runtime base image (pin this to whatever Runtime you use locally)
FROM quay.io/astronomer/astro-runtime:11.3.0-base

USER root

# OS-level packages
COPY packages.txt /packages.txt
RUN apt-get update &&     apt-get install -y --no-install-recommends $(cat /packages.txt) &&     apt-get clean && rm -rf /var/lib/apt/lists/*

# Install dbt into an isolated venv (recommended to avoid dependency conflicts)
RUN python -m venv /usr/local/airflow/dbt_venv &&     /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-bigquery==1.8.3

# Python deps for DAG utilities (BigQuery client + RDF export)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER astro
