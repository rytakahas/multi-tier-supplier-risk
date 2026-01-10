# Optional Neo4j projection (RDF → Neo4j via n10s)

This repo's default GraphDB is **Fuseki (SPARQL)**.

If you want Neo4j for Cypher/GraphRAG:
1) Add a Neo4j service to docker-compose (optional)
2) Install/enable **neosemantics (n10s)**
3) Import `data/kg/supplychain.ttl` using `n10s.rdf.import.fetch`

We keep this optional to avoid extra containers for the default path.
