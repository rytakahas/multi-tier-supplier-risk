import os
import argparse
import requests


def load_ttl(fuseki_url: str, dataset: str, ttl_path: str):
    # POST Turtle to dataset data endpoint
    url = f"{fuseki_url}/{dataset}/data"
    with open(ttl_path, "rb") as f:
        r = requests.post(url, headers={"Content-Type": "text/turtle"}, data=f)
    r.raise_for_status()
    print(f"Loaded TTL into Fuseki dataset '{dataset}'")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fuseki-url", default=os.environ.get("FUSEKI_URL", "http://localhost:3030"))
    ap.add_argument("--dataset", default=os.environ.get("FUSEKI_DATASET", "sc"))
    ap.add_argument("--ttl", required=True)
    args = ap.parse_args()
    load_ttl(args.fuseki_url, args.dataset, args.ttl)


if __name__ == "__main__":
    main()
