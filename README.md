## multi-tier-supplier-risk-graphrag

An end-to-end **Supply Chain Risk / Provenance** reference pipeline that starts from a **Data Lake (BigQuery raw)**,
builds a **DWH (BigQuery + dbt star schema)**, branches into an **RDF Knowledge Graph (Fuseki SPARQL)**,
optionally projects into **Neo4j**, and serves **GraphRAG** answers like:

> “If supplier **X** fails, what **products** and **regions** are impacted — and why?”

This repo is designed for your setup:
- **Data Lake (raw):** `vivid-layout-453307-p4.ryoji_raw_demos`
- **DWH (warehouse):** `able-balm-454718-n8.ryoji_wh_demos`
- Local machine: **MacBook Air M3**
- You prefer: **Docker only** (no local installs)

---

### Architecture

```
Sources → BigQuery Raw (DL) → dbt (staging/marts) → BigQuery DWH (Gold)
                                              ↘︎ RDF export (Turtle) → Fuseki (SPARQL GraphDB)
                                                    ↘︎ optional → Neo4j (property-graph projection)
Fuseki/Neo4j subgraph retrieval → LLM summarization (Hugging Face) → GraphRAG API
```

#### Why DWH first?
Most businesses keep metrics + reporting in the DWH. The KG is a **derived semantic layer** that is rebuilt from marts.
This repo treats:
- **DWH = system of record**
- **KG = reproducible branch**

---

### Repo layout

```
multi-tier-supplier-risk-graphrag/
├─ data/
│  └─ raw/                         # sample CSVs to load into BigQuery raw dataset
├─ dbt_supplychain/                # dbt project (BigQuery)
│  ├─ dbt_project.yml
│  ├─ packages.yml
│  ├─ models/
│  │  ├─ sources.yml
│  │  ├─ staging/
│  │  └─ marts/
│  └─ macros/
├─ airflow/
│  └─ dags/
│     └─ supplychain_kg_pipeline.py
├─ kg/
│  ├─ ontology/                    # ontology (TBox) in Turtle (export from Protégé)
│  ├─ export/                      # DWH marts → RDF (ABox) exporter
│  ├─ load/                        # loaders for Fuseki (and optional Neo4j)
│  └─ queries/                     # SPARQL templates
├─ services/
│  └─ graphrag_api/                # FastAPI GraphRAG service (SPARQL + HF model)
├─ docker/
│  ├─ airflow/                     # Airflow image with dbt-bigquery + GCP libs
│  └─ graphrag/                    # GraphRAG API image
├─ scripts/                        # helper scripts (BQ load, demos)
├─ docker-compose.yml
└─ .env.example
```

---

### Datasets and tables

#### Raw (Data Lake) tables (load these CSVs into `ryoji_raw_demos`)
- `suppliers`
- `regions`
- `facilities`
- `products`
- `parts`
- `part_subcomponents` (multi-tier dependency)
- `product_components` (BOM mapping part → product)
- `supplier_parts` (supplier → parts supplied)
- `supplier_facilities` (supplier → delivery facilities)
- `shipments` (operational shipments, lead time, status)
- `disruptions` (events: fire/port congestion/etc.)

#### DWH (star schema) in `ryoji_wh_demos`
**Dimensions**
- `dim_supplier`
- `dim_part`
- `dim_product`
- `dim_facility`
- `dim_region`

**Facts**
- `f_shipment`
- `f_bom_component` (product ↔ part usage)
- `f_part_dependency` (part ↔ subcomponent usage)
- `f_disruption`

---

### Quick start (Docker-only)

#### 0) Prereqs (host)
You only need:
- Docker + Docker Compose
- `gcloud` authenticated for BigQuery (ADC)
  - `gcloud auth application-default login`

> We mount your `~/.config/gcloud` into containers so Python/dbt can access BigQuery via ADC.

#### 1) Configure env
Copy the example env file:

```bash
cp .env.example .env
```

Edit `.env` to match:
- `BQ_RAW_PROJECT=vivid-layout-453307-p4`
- `BQ_RAW_DATASET=ryoji_raw_demos`
- `BQ_WH_PROJECT=able-balm-454718-n8`
- `BQ_WH_DATASET=ryoji_wh_demos`

#### 2) Start services
This brings up:
- Airflow (with dbt)
- Fuseki (SPARQL GraphDB)
- GraphRAG API (FastAPI)

```bash
docker compose up -d --build
```

Airflow UI: http://localhost:8080  
Fuseki UI: http://localhost:3030  
GraphRAG API: http://localhost:8000/docs

#### 3) Run the pipeline
In Airflow:
- enable DAG: `supplychain_kg_pipeline`
- trigger run

What it does:
1) Load sample CSVs → BigQuery raw (`ryoji_raw_demos`)
2) Run dbt: staging → marts → publish star schema to `ryoji_wh_demos`
3) Export RDF Turtle `data/kg/supplychain.ttl` from marts
4) Load TTL into Fuseki dataset `sc`

---

### GraphRAG usage (example)

#### Ask: supplier impact
Once the DAG ran, call:

```bash
curl -X POST http://localhost:8000/impact   -H "Content-Type: application/json"   -d '{"supplier_name":"Astra Components"}'
```

You’ll get:
- impacted products
- impacted regions
- evidence triples (why)
- LLM narrative summary (Hugging Face model)

---

### Modeling in the KG (thumb rules)

- **Entities with stable IDs** become **nodes**: Supplier, Part, Product, Facility, Region, DisruptionEvent
- **Relationships** become predicates: supplies, usedIn, subcomponentOf, deliversTo, locatedIn
- Keep DWH facts as either:
  - event nodes (Shipment, Disruption) when you need many links, OR
  - edge properties when it’s a simple traversal

---

### Notes on Hugging Face model choice

Default is a small CPU-friendly summarizer:
- `google/flan-t5-base`

You can change via `.env`:
- `HF_MODEL_NAME=...`
Optionally set `HUGGINGFACE_TOKEN` if you hit rate limits.

---

### What to build next (production hardening)
- SHACL validation on the exported TTL
- incremental graph updates (partition by date)
- entity resolution (supplier name normalization)
- streaming path with Kafka → micro-batch → SPARQL Update

---

### License
MIT


---

### Run with Astro CLI (`astro dev start`)

#### 0) Authenticate BigQuery ADC on your host
```bash
gcloud auth application-default login
```

#### 1) Start Airflow + extra services
```bash
astro dev start
```

Astro starts Airflow. `docker-compose.override.yml` adds:
- Fuseki (SPARQL GraphDB)
- GraphRAG API

#### 2) Trigger the DAG
The DAG is manual (`schedule=None`):
- Airflow UI → DAG `supplychain_kg_pipeline` → Trigger DAG

#### 3) Ask GraphRAG
```bash
curl -X POST http://localhost:8000/impact   -H "Content-Type: application/json"   -d '{"supplier_name":"Astra Components"}'
```
