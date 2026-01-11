import os
import argparse
import requests


def load_ttl(fuseki_url: str, dataset: str, ttl_path: str, timeout_s: int = 60):
    url = f"{fuseki_url.rstrip('/')}/{dataset}/data"

    # Auth (needed if ADMIN_PASSWORD is set on Fuseki)
    user = os.environ.get("FUSEKI_USER", "admin")
    password = os.environ.get("FUSEKI_PASSWORD", "")
    auth = (user, password) if password else None

    with open(ttl_path, "rb") as f:
        r = requests.post(
            url,
            headers={"Content-Type": "text/turtle"},
            data=f,
            auth=auth,
            timeout=timeout_s,
        )

    if not r.ok:
        raise RuntimeError(
            f"Fuseki load failed: HTTP {r.status_code} for {url}\n"
            f"Response:\n{r.text[:2000]}"
        )

    print(f"Loaded TTL into Fuseki dataset '{dataset}' via {url}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--fuseki-url",
        default=os.environ.get("FUSEKI_URL", "http://host.docker.internal:3030"),
        help="Fuseki base URL. On Mac from inside containers: http://host.docker.internal:3030",
    )
    ap.add_argument(
        "--dataset",
        default=os.environ.get("FUSEKI_DATASET", "sc"),
        help="Fuseki dataset name.",
    )
    ap.add_argument("--ttl", required=True, help="Path to Turtle file to upload.")
    ap.add_argument("--timeout-s", type=int, default=60)
    args = ap.parse_args()

    load_ttl(args.fuseki_url, args.dataset, args.ttl, timeout_s=args.timeout_s)


if __name__ == "__main__":
    main()
