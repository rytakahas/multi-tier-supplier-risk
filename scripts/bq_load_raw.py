import os
import glob
import argparse
from pathlib import Path

from google.cloud import bigquery


def ensure_dataset(client: bigquery.Client, project: str, dataset: str, location: str = "EU"):
    ds_id = f"{project}.{dataset}"
    try:
        client.get_dataset(ds_id)
    except Exception:
        ds = bigquery.Dataset(ds_id)
        ds.location = location
        client.create_dataset(ds, exists_ok=True)


def load_csvs(raw_dir: str, project: str, dataset: str):
    client = bigquery.Client(project=project)
    ensure_dataset(client, project, dataset)

    for csv_path in sorted(glob.glob(str(Path(raw_dir) / "*.csv"))):
        table_name = Path(csv_path).stem
        table_id = f"{project}.{dataset}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(csv_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {csv_path} -> {table_id} ({job.output_rows} rows)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", default="/opt/project/data/raw")
    ap.add_argument("--project", default=os.environ["BQ_RAW_PROJECT"])
    ap.add_argument("--dataset", default=os.environ["BQ_RAW_DATASET"])
    args = ap.parse_args()
    load_csvs(args.raw_dir, args.project, args.dataset)


if __name__ == "__main__":
    main()
