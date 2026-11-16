# Cria ou atualiza a conexao legado_pg a partir da connection string copiada (area de transferencia do macOS).
# Uso: copie a string postgresql://... no provedor e rode: source scripts/federacao_conexao.sh && federacao_conexao
# A string nao passa pelo terminal nem pelo historico; a area de transferencia e limpa ao final.
federacao_conexao() {
  local perfil="${DATABRICKS_PROFILE:-dbt-lakehouse-free}" cfg rc acao
  cfg="$(mktemp)"
  chmod 600 "$cfg"
  if databricks connections get legado_pg --profile "$perfil" >/dev/null 2>&1; then acao=update; else acao=create; fi
  pbpaste | python3 -c '
import json, sys
from urllib.parse import urlparse, unquote
acao, destino = sys.argv[1], sys.argv[2]
u = urlparse(sys.stdin.read().strip().strip("\x27\""))
faltando = [n for n, v in (("postgresql://", u.scheme in ("postgresql", "postgres")), ("usuario", u.username), ("senha", u.password), ("host", u.hostname)) if not v]
if faltando:
    sys.exit("PARAR: area de transferencia sem connection string valida, falta: " + ", ".join(faltando))
opcoes = {"host": u.hostname.replace("-pooler.", "."), "port": str(u.port or 5432), "user": unquote(u.username), "password": unquote(u.password)}
cfg = {"options": opcoes} if acao == "update" else {"name": "legado_pg", "connection_type": "POSTGRESQL", "comment": "Sistema legado externo (PostgreSQL) - metas comerciais", "options": opcoes}
open(destino, "w").write(json.dumps(cfg))
print("host:", opcoes["host"], "usuario:", opcoes["user"])
' "$acao" "$cfg"
  rc=$?
  printf "" | pbcopy
  if [ "$rc" -eq 0 ]; then
    if [ "$acao" = update ]; then
      databricks connections update legado_pg --json @"$cfg" --profile "$perfil" >/dev/null
    else
      databricks connections create --json @"$cfg" --profile "$perfil" >/dev/null
    fi
    rc=$?
  fi
  rm -f "$cfg"
  [ "$rc" -eq 0 ] && echo "conexao legado_pg: $acao ok" || { echo "PARAR: conexao legado_pg ($acao) falhou"; return 1; }
}
