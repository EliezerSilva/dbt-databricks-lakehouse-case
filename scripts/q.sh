# Consulta SQL no warehouse do projeto pela API de statements.
# Uso: source scripts/q.sh && q "SELECT 1"
# Variaveis opcionais: DATABRICKS_PROFILE (padrao dbt-lakehouse-free), WAREHOUSE_ID (padrao 6ad31e4709e166fb).
q() {
  databricks api post /api/2.0/sql/statements --profile "${DATABRICKS_PROFILE:-dbt-lakehouse-free}" \
    --json "$(python3 -c 'import json,sys; print(json.dumps({"warehouse_id":sys.argv[2],"wait_timeout":"50s","statement":sys.argv[1]}))' "$1" "${WAREHOUSE_ID:-6ad31e4709e166fb}")" \
  | python3 -c "import json,sys; r=json.load(sys.stdin); print(r['status']['state'], r['status'].get('error',{}).get('message','')); [print(x) for x in (r.get('result',{}).get('data_array') or [])]"
}
