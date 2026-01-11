#!/usr/bin/env bash
set -euo pipefail

SUPPLIER="${1:?Usage: scripts/impact_agent.sh 'Supplier Name'}"

curl -s -X POST http://localhost:8000/impact \
  -H "Content-Type: application/json" \
  -d "{\"supplier_name\":\"${SUPPLIER}\"}" > /tmp/impact.json

cat /tmp/impact.json | docker exec -i sc_gemini_cli gemini -p \
"Write an executive risk memo based ONLY on this JSON.
Include impacted products, impacted regions, reasoning, mitigations."

