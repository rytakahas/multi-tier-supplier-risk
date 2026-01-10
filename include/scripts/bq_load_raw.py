import os
import glob
import argparse
from pathlib import Path

from google.cloud import bigquery


# Explicit schemas for the tables that dbt is failing on
SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    "parts": [
        bigquery.SchemaField("part_id", "STRING"),
        bigquery.SchemaField("part_name", "STRING"),
        bigquery.SchemaField("criticality", "STRING"),
    ],
    "products": [
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("category", "STRING"),
    ],
    "facilities": [
        bigquery.SchemaField("facility_id", "STRING"),
        bigquery.SchemaField("facility_name", "STRING"),
        bigquery.SchemaField("facility_type", "STRING"),
        bigquery.SchemaField("region_id", "STRING"),
    ],
    "regions": [
        bigquery.SchemaField("region_id", "STRING"),
        bigquery.SchemaField("region_name", "STRING"),
        bigquery.SchemaField("country_code", "STRING"),
    ],
    "supplier_parts": [
        bigquery.SchemaField("supplier_id", "STRING"),
        bigquery.SchemaField("part_id", "STRING"),
    ],
    "supplier_facilities": [
        bigquery.SchemaField("supplier_id", "STRING"),
        bigquery.SchemaField("facility_id", "STRING"),
    ],
}


def ensure_dataset(client: bigquery.Client, project: str, dataset: str, location: str):
    ds_id = f"{project}.{dataset}"
    try:
        client.get_dataset(ds_id)
        return
    except Exception:
        ds = bigquery.Dataset(ds_id)
        ds.location = location
        client.create_dataset(ds, exists_ok=True)
        print(f"Created dataset: {ds_id} (location={location})")


def load_csvs(raw_dir: str, project: str, dataset: str, location: str):
    client = bigquery.Client(project=project)
    ensure_dataset(client, project, dataset, location)

    csv_files = sorted(glob.glob(str(Path(raw_dir) / "*.csv")))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in raw_dir={raw_dir}")

    for csv_path in csv_files:
        table_name = Path(csv_path).stem
        table_id = f"{project}.{dataset}.{table_name}"

        # Always delete to avoid stale schemas
        client.delete_table(table_id, not_found_ok=True)

        schema = SCHEMAS.get(table_name)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            field_delimiter=",",
            encoding="UTF-8",
        )

        if schema:
            job_config.schema = schema
            job_config.autodetect = False
        else:
            job_config.autodetect = True

        with open(csv_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)

        job.result()
        print(f"Loaded {csv_path} -> {table_id} ({job.output_rows} rows)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--raw-dir",
        default="/usr/local/airflow/include/data/raw",
        help="Directory containing raw CSV files (inside Astro containers).",
    )
    ap.add_argument("--project", default=os.environ["BQ_RAW_PROJECT"])
    ap.add_argument("--dataset", default=os.environ["BQ_RAW_DATASET"])
    ap.add_argument("--location", default=os.environ.get("BQ_LOCATION", "europe-west1"))
    args = ap.parse_args()

    load_csvs(args.raw_dir, args.project, args.dataset, args.location)


if __name__ == "__main__":
    main()
