import json
import textwrap
from typing import Any, Dict, List, Optional

from SPARQLWrapper import SPARQLWrapper, JSON
from transformers import pipeline


PREFIXES = """
PREFIX scr: <https://example.org/supplychain/kg#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
""".strip()


def _sparql_select(endpoint: str, query: str) -> List[Dict[str, Any]]:
    sp = SPARQLWrapper(endpoint)
    sp.setQuery(query)
    sp.setReturnFormat(JSON)

    # Some SPARQLWrapper builds may not have setTimeout; keep safe.
    if hasattr(sp, "setTimeout"):
        sp.setTimeout(30)

    try:
        res = sp.query().convert()
    except Exception as e:
        raise RuntimeError(f"SPARQL query failed against {endpoint}: {e}")

    return res.get("results", {}).get("bindings", [])


def _get_supplier_uri(endpoint: str, supplier_name: str) -> Optional[str]:
    q = PREFIXES + f"""
SELECT ?s WHERE {{
  ?s a scr:Supplier ;
     rdfs:label ?lbl .
  FILTER(LCASE(STR(?lbl)) = LCASE({json.dumps(supplier_name)}))
}} LIMIT 5
"""
    rows = _sparql_select(endpoint, q)
    return rows[0]["s"]["value"] if rows else None


def _top_impacts(
    endpoint: str,
    supplier_uri: str,
    top_k_parts: int,
    top_k_products: int,
    top_k_regions: int,
):
    # Parts directly supplied
    q_parts = PREFIXES + f"""
SELECT DISTINCT ?part ?partLabel WHERE {{
  <{supplier_uri}> scr:supplies ?part .
  OPTIONAL {{ ?part rdfs:label ?partLabel }}
}} LIMIT {int(top_k_parts)}
"""

    # Products impacted via multi-tier dependency:
    # supplier supplies part -> (subcomponentOf)* -> basePart -> usedIn -> product
    q_products = PREFIXES + f"""
SELECT DISTINCT ?product ?productLabel ?basePart ?basePartLabel WHERE {{
  <{supplier_uri}> scr:supplies ?part .
  ?part (scr:subcomponentOf)* ?basePart .
  ?basePart scr:usedIn ?product .
  OPTIONAL {{ ?product rdfs:label ?productLabel }}
  OPTIONAL {{ ?basePart rdfs:label ?basePartLabel }}
}} LIMIT {int(top_k_products)}
"""

    # Regions impacted via deliveries: supplier -> deliversTo facility -> locatedIn region
    q_regions = PREFIXES + f"""
SELECT DISTINCT ?region ?regionLabel ?facility ?facilityLabel WHERE {{
  <{supplier_uri}> scr:deliversTo ?facility .
  ?facility scr:locatedIn ?region .
  OPTIONAL {{ ?region rdfs:label ?regionLabel }}
  OPTIONAL {{ ?facility rdfs:label ?facilityLabel }}
}} LIMIT {int(top_k_regions)}
"""

    parts = _sparql_select(endpoint, q_parts)
    products = _sparql_select(endpoint, q_products)
    regions = _sparql_select(endpoint, q_regions)
    return parts, products, regions


def _format_evidence(parts, products, regions) -> str:
    def lbl(row, uri_key, label_key):
        uri = row[uri_key]["value"]
        return row.get(label_key, {}).get("value", uri.split("/")[-1])

    lines = []
    lines.append("EVIDENCE (triples-derived facts):")
    lines.append("- Directly supplied parts:")
    for r in parts:
        lines.append(f"  - {lbl(r, 'part', 'partLabel')}")
    lines.append("- Impacted products (multi-tier):")
    for r in products:
        prod = lbl(r, "product", "productLabel")
        base = lbl(r, "basePart", "basePartLabel")
        lines.append(f"  - {prod} impacted via component {base}")
    lines.append("- Impacted regions (delivery footprint):")
    for r in regions:
        reg = lbl(r, "region", "regionLabel")
        fac = lbl(r, "facility", "facilityLabel")
        lines.append(f"  - {reg} (via {fac})")
    return "\n".join(lines)


def _llm_summarize(model_name: str, token: Optional[str], supplier_name: str, evidence: str) -> str:
    prompt = textwrap.dedent(f"""
    You are a supply-chain risk analyst.
    Task: explain the impact if supplier '{supplier_name}' fails.
    Use only the evidence below. Be concrete and list impacted products and regions with the dependency logic.

    {evidence}

    Output format:
    1) Impacted products (bullets)
    2) Impacted regions (bullets)
    3) Reasoning path (short)
    4) Mitigations (3-5 bullets)
    """).strip()

    gen = pipeline(
        "text2text-generation",
        model=model_name,
        tokenizer=model_name,
        token=token,
    )
    out = gen(prompt, max_length=512, do_sample=False)
    return out[0]["generated_text"]


def impact_analysis(
    supplier_name: str,
    top_k_parts: int,
    top_k_products: int,
    top_k_regions: int,
    sparql_endpoint: Optional[str],
    hf_model: str,
    hf_token: Optional[str],
) -> Dict[str, Any]:
    if not sparql_endpoint:
        raise RuntimeError("SPARQL_ENDPOINT env var not set")

    supplier_uri = _get_supplier_uri(sparql_endpoint, supplier_name)
    if not supplier_uri:
        return {
            "error": f"Supplier not found in KG: {supplier_name}",
            "hint": "Check exact label in suppliers.csv or confirm KG load into Fuseki.",
        }

    parts, products, regions = _top_impacts(
        sparql_endpoint, supplier_uri, top_k_parts, top_k_products, top_k_regions
    )

    evidence = _format_evidence(parts, products, regions)

    # Make LLM optional: still return graph results even if HF fails
    try:
        summary = _llm_summarize(hf_model, hf_token, supplier_name, evidence)
    except Exception as e:
        summary = f"(LLM summarization failed: {e})"

    def _label(row, uri_key, label_key):
        uri = row[uri_key]["value"]
        return row.get(label_key, {}).get("value", uri.split("/")[-1])

    return {
        "supplier": {"name": supplier_name, "uri": supplier_uri},
        "impacted_parts": [
            {"uri": r["part"]["value"], "label": _label(r, "part", "partLabel")}
            for r in parts
        ],
        "impacted_products": [
            {
                "uri": r["product"]["value"],
                "label": _label(r, "product", "productLabel"),
                "via_component": _label(r, "basePart", "basePartLabel"),
            }
            for r in products
        ],
        "impacted_regions": [
            {
                "uri": r["region"]["value"],
                "label": _label(r, "region", "regionLabel"),
                "via_facility": _label(r, "facility", "facilityLabel"),
            }
            for r in regions
        ],
        "evidence": evidence,
        "llm_summary": summary,
    }
