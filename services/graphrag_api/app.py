import os
from fastapi import FastAPI
from pydantic import BaseModel
from rag import impact_analysis

app = FastAPI(title="Supply Chain GraphRAG API", version="0.1.0")

class ImpactRequest(BaseModel):
    supplier_name: str
    top_k_parts: int = 10
    top_k_products: int = 10
    top_k_regions: int = 10

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/impact")
def impact(req: ImpactRequest):
    return impact_analysis(
        supplier_name=req.supplier_name,
        top_k_parts=req.top_k_parts,
        top_k_products=req.top_k_products,
        top_k_regions=req.top_k_regions,
        sparql_endpoint=os.environ.get("SPARQL_ENDPOINT"),
        hf_model=os.environ.get("HF_MODEL_NAME", "google/flan-t5-base"),
        hf_token=os.environ.get("HUGGINGFACE_TOKEN") or None,
    )
