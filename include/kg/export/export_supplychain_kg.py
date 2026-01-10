import os
import argparse
from datetime import date
from typing import Dict

import pandas as pd
from google.cloud import bigquery
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, RDFS, XSD


SCR = Namespace("https://example.org/supplychain/kg#")


def _bq_client():
    # Uses Application Default Credentials (ADC).
    return bigquery.Client(project=os.environ["BQ_WH_PROJECT"])


def _read_table(client: bigquery.Client, project: str, dataset: str, table: str) -> pd.DataFrame:
    q = f"SELECT * FROM `{project}.{dataset}.{table}`"
    return client.query(q).to_dataframe()


def export_ttl(out_path: str):
    project = os.environ["BQ_WH_PROJECT"]
    dataset = os.environ["BQ_WH_DATASET"]

    client = _bq_client()

    dim_supplier = _read_table(client, project, dataset, "dim_supplier")
    dim_part = _read_table(client, project, dataset, "dim_part")
    dim_product = _read_table(client, project, dataset, "dim_product")
    dim_facility = _read_table(client, project, dataset, "dim_facility")
    dim_region = _read_table(client, project, dataset, "dim_region")

    f_bom = _read_table(client, project, dataset, "f_bom_component")
    f_dep = _read_table(client, project, dataset, "f_part_dependency")
    f_ship = _read_table(client, project, dataset, "f_shipment")
    f_disr = _read_table(client, project, dataset, "f_disruption")

    g = Graph()
    g.bind("scr", SCR)
    g.bind("rdfs", RDFS)

    # Entities
    def uri(cls: str, key: str):
        return SCR[f"{cls}/{key}"]

    # Suppliers
    for _, r in dim_supplier.iterrows():
        s = uri("Supplier", r["supplier_key"])
        g.add((s, RDF.type, SCR.Supplier))
        g.add((s, RDFS.label, Literal(r["supplier_name"])))
        g.add((s, SCR["tier"], Literal(int(r["tier"]), datatype=XSD.integer)))
        g.add((s, SCR["countryCode"], Literal(r["country_code"])))

    # Parts
    for _, r in dim_part.iterrows():
        p = uri("Part", r["part_key"])
        g.add((p, RDF.type, SCR.Part))
        g.add((p, RDFS.label, Literal(r["part_name"])))
        g.add((p, SCR["criticality"], Literal(r["criticality"])))

    # Products
    for _, r in dim_product.iterrows():
        pr = uri("Product", r["product_key"])
        g.add((pr, RDF.type, SCR.Product))
        g.add((pr, RDFS.label, Literal(r["product_name"])))
        g.add((pr, SCR["category"], Literal(r["category"])))

    # Regions
    for _, r in dim_region.iterrows():
        rg = uri("Region", r["region_key"])
        g.add((rg, RDF.type, SCR.Region))
        g.add((rg, RDFS.label, Literal(r["region_name"])))
        g.add((rg, SCR["countryCode"], Literal(r["country_code"])))

    # Facilities
    for _, r in dim_facility.iterrows():
        f = uri("Facility", r["facility_key"])
        g.add((f, RDF.type, SCR.Facility))
        g.add((f, RDFS.label, Literal(r["facility_name"])))
        g.add((f, SCR["facilityType"], Literal(r["facility_type"])))
        rg = uri("Region", r["region_key"])
        g.add((f, SCR.locatedIn, rg))

    # BOM: Part used in Product
    for _, r in f_bom.iterrows():
        part = uri("Part", r["part_key"])
        prod = uri("Product", r["product_key"])
        g.add((part, SCR.usedIn, prod))
        g.add((part, SCR["bomQty"], Literal(int(r["qty"]), datatype=XSD.integer)))

    # Multi-tier dependencies (child -> parent via subcomponentOf)
    for _, r in f_dep.iterrows():
        parent = uri("Part", r["parent_part_key"])
        child = uri("Part", r["child_part_key"])
        # Child is a subcomponent of parent (child -> parent)
        g.add((child, SCR.subcomponentOf, parent))
        g.add((child, SCR["depQty"], Literal(int(r["qty"]), datatype=XSD.integer)))

    # Shipments: link supplier supplies part; supplier delivers to facility
    for _, r in f_ship.iterrows():
        sup = uri("Supplier", r["supplier_key"])
        part = uri("Part", r["part_key"])
        fac = uri("Facility", r["facility_key"])
        g.add((sup, SCR.supplies, part))
        g.add((sup, SCR.deliversTo, fac))

        # shipment event as reified node (optional, kept minimal)
        sh = uri("Shipment", r["shipment_id"])
        g.add((sh, RDF.type, SCR["Shipment"]))
        g.add((sh, RDFS.label, Literal(f"Shipment {r['shipment_id']}")))
        g.add((sh, SCR["shipDate"], Literal(str(r["ship_date"]), datatype=XSD.date)))
        g.add((sh, SCR["qty"], Literal(int(r["qty"]), datatype=XSD.integer)))
        g.add((sh, SCR["leadTimeDays"], Literal(int(r["lead_time_days"]), datatype=XSD.integer)))
        g.add((sh, SCR["status"], Literal(r["status"])))
        g.add((sh, SCR["fromSupplier"], sup))
        g.add((sh, SCR["toFacility"], fac))
        g.add((sh, SCR["forPart"], part))

    # Disruptions
    for _, r in f_disr.iterrows():
        sup = uri("Supplier", r["supplier_key"])
        d = uri("Disruption", r["disruption_id"])
        g.add((d, RDF.type, SCR.Disruption))
        g.add((d, RDFS.label, Literal(f"{r['disruption_type']} ({r['disruption_id']})")))
        g.add((d, SCR["startDate"], Literal(str(r["start_date"]), datatype=XSD.date)))
        g.add((d, SCR["endDate"], Literal(str(r["end_date"]), datatype=XSD.date)))
        g.add((d, SCR["severity"], Literal(float(r["severity"]), datatype=XSD.decimal)))
        g.add((sup, SCR.hasDisruption, d))

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    g.serialize(destination=out_path, format="turtle")
    print(f"Wrote TTL: {out_path} (triples={len(g)})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output TTL path")
    args = ap.parse_args()
    export_ttl(args.out)


if __name__ == "__main__":
    main()
