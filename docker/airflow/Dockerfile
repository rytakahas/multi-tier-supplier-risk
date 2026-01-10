FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# dbt-bigquery + minimal data libs + RDF export libs (no heavy LLM here)
RUN pip install --no-cache-dir \
    dbt-bigquery==1.8.3 \
    google-cloud-bigquery==3.25.0 \
    pandas==2.2.2 \
    pyarrow==16.1.0 \
    rdflib==7.0.0 \
    requests==2.32.3
