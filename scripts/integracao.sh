#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "1/4 testes do agente"
python -m unittest discover -s tests/agent

echo "2/4 dbt build (target dev)"
(cd dbt && dbt build)

echo "3/4 ontologia contra o manifest"
python -m agent.ontologia --manifest dbt/target/manifest.json

echo "4/4 agente no ambiente original, sem LLM e sem gravar"
python -m agent.run --ambiente original --dbt-target-dir dbt/target --sem-llm > /tmp/integracao_agente.json
python - <<'PY'
import json
d = json.load(open("/tmp/integracao_agente.json"))
assert d["status"] == "OK", d["avisos"]
print(f"agente OK: {len(d['diagnosticos'])} incidentes, avisos={d['avisos']}")
PY

echo "integracao OK"
