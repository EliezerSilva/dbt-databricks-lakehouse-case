# Simulacao do reprocessamento de itens orfaos em schemas isolados (dbt_sim_*), sem tocar no ambiente real (dbt_*).
# Uso: source scripts/simular_cabecalho_tardio.sh && sim_dbt <comando dbt>
#   sim_dbt build --exclude resource_type:test                                    (sem cabecalho tardio)
#   sim_dbt build --exclude resource_type:test --vars '{simular_cabecalho_tardio: true}'
# O profiles.yml temporario copia o target padrao de ~/.dbt/profiles.yml trocando so o schema para dbt_sim.
sim_dbt() {
  local raiz tmp
  raiz="$(git rev-parse --show-toplevel 2>/dev/null)" || { echo "PARAR: fora do repositorio"; return 1; }
  tmp="$(mktemp -d)"
  python3 - "$tmp" <<'PY' || { rm -rf "$tmp"; return 1; }
import sys, yaml
from pathlib import Path
perfis = yaml.safe_load(Path.home().joinpath(".dbt", "profiles.yml").read_text())
p = perfis["lakehouse_case"]
base = dict(p["outputs"][p.get("target", "dev")])
if base.get("schema") != "dbt":
    sys.exit("PARAR: o target padrao deveria usar o schema dbt; encontrado " + str(base.get("schema")))
base["schema"] = "dbt_sim"
destino = Path(sys.argv[1]) / "profiles.yml"
destino.write_text(yaml.safe_dump({"lakehouse_case": {"target": "sim", "outputs": {"sim": base}}}))
destino.chmod(0o600)
PY
  (cd "$raiz/dbt" && dbt "$@" --profiles-dir "$tmp")
  local rc=$?
  rm -rf "$tmp"
  return $rc
}
