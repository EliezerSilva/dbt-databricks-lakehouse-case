# Cria ou recria a conexao legado_pg e o catalogo estrangeiro a partir do .env baixado do provedor do PostgreSQL.
# Uso: source scripts/federacao_conexao.sh && federacao_conexao [caminho_do_env]   (padrao: ~/Downloads/env.txt)
# O .env precisa de PGUSER e PGPASSWORD (PGHOST e PGDATABASE opcionais; DATABASE_URL tambem e aceito).
# Sem PGHOST, usa o host da conexao existente ou a variavel FEDERACAO_HOST.
# A senha e testada a partir da maquina local antes de qualquer alteracao no Databricks.
# Rotacao: recriar e deterministico; "connections update" respondeu ok sem aplicar a nova senha (2026-11-16).
source "$(dirname "${BASH_SOURCE[0]:-$0}")/q.sh"

federacao_conexao() {
  local env_file="${1:-$HOME/Downloads/env.txt}" perfil="${DATABRICKS_PROFILE:-dbt-lakehouse-free}" cfg host_atual rc
  [ -f "$env_file" ] || { echo "PARAR: arquivo nao encontrado: $env_file"; return 1; }
  host_atual="$(databricks connections get legado_pg --profile "$perfil" -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("options",{}).get("host",""))' 2>/dev/null)"
  cfg="$(mktemp)"
  chmod 600 "$cfg"
  python3 - "$env_file" "$cfg" "${FEDERACAO_HOST:-$host_atual}" <<'PY'
import json, re, sys
from pathlib import Path
from urllib.parse import urlparse, unquote
env_file, destino, host_padrao = sys.argv[1:4]
txt = Path(env_file).read_text()
v = dict(re.findall(r"^\s*(?:export\s+)?(\w+)\s*[=:]\s*['\"]?([^'\"\n]*)['\"]?\s*$", txt, re.M))
url = next((x for x in v.values() if x.startswith(("postgresql://", "postgres://"))), None)
if url:
    u = urlparse(url)
    host, user, pwd, db = u.hostname, unquote(u.username or ""), unquote(u.password or ""), u.path.lstrip("/") or "neondb"
else:
    host, user, pwd, db = v.get("PGHOST") or host_padrao, v.get("PGUSER", ""), v.get("PGPASSWORD", ""), v.get("PGDATABASE") or "neondb"
host = (host or "").replace("-pooler.", ".")
faltando = [n for n, x in (("host", host), ("usuario", user), ("senha", pwd)) if not x]
if faltando:
    sys.exit("PARAR: faltando no .env: " + ", ".join(faltando))
try:
    import psycopg
except ImportError:
    sys.exit("PARAR: driver ausente; rode: python3 -m pip install 'psycopg[binary]==3.2.13'")
try:
    with psycopg.connect(host=host, dbname=db, user=user, password=pwd, sslmode="require", connect_timeout=15) as c:
        c.execute("SELECT 1")
except Exception as e:
    sys.exit("PARAR: credencial recusada pelo PostgreSQL (nada alterado no Databricks): " + type(e).__name__)
json.dump({"name": "legado_pg", "connection_type": "POSTGRESQL",
           "comment": "Sistema legado externo (PostgreSQL) - metas comerciais",
           "options": {"host": host, "port": "5432", "user": user, "password": pwd}}, open(destino, "w"))
print(f"credencial valida | host: {host} | usuario: {user} | database: {db}")
json.dump({"database": db}, open(destino + ".db", "w"))
PY
  rc=$?
  if [ "$rc" -ne 0 ]; then rm -f "$cfg" "$cfg.db"; return 1; fi
  local db
  db="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["database"])' "$cfg.db")"
  rm -f "$cfg.db"
  q "DROP CATALOG IF EXISTS legado_pg CASCADE"
  databricks connections delete legado_pg --profile "$perfil" >/dev/null 2>&1
  databricks connections create --json @"$cfg" --profile "$perfil" >/dev/null
  rc=$?
  rm -f "$cfg"
  [ "$rc" -eq 0 ] || { echo "PARAR: criacao da conexao legado_pg falhou"; return 1; }
  q "CREATE FOREIGN CATALOG IF NOT EXISTS legado_pg USING CONNECTION legado_pg OPTIONS (database '$db') COMMENT 'Catalogo federado do sistema legado de metas'"
  q "SELECT count(*) AS linhas FROM legado_pg.public.metas_comerciais"
}
