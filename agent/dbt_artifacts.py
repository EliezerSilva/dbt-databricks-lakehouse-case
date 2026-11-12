import json
import re
from pathlib import Path

TARGET_POR_AMBIENTE = {"original": "dev", "cenarios": "cenarios"}
_REL = re.compile(r"^`workspace`\.`dbt_[a-z0-9_]+`\.`[a-z0-9_]+`$")
_COL = re.compile(r"^[a-z_][a-z0-9_]*$")


def carregar(target_dir):
    d = Path(target_dir)
    return (json.loads((d / "run_results.json").read_text()),
            json.loads((d / "manifest.json").read_text()))


def target_de(run_results):
    return (run_results.get("args") or {}).get("target") or "dev"


def validar_identificadores(relacao, coluna):
    if not _REL.match(relacao or "") or not _COL.match(coluna or ""):
        raise ValueError(f"identificador fora do padrao permitido: {relacao} / {coluna}")


def falhas(run_results, manifest):
    nodes = manifest.get("nodes", {})
    out = []
    for r in run_results.get("results", []):
        if r.get("status") != "fail":
            continue
        node = nodes.get(r["unique_id"], {})
        meta = node.get("test_metadata") or {}
        kwargs = meta.get("kwargs") or {}
        modelo_id = node.get("attached_node") or next(iter((node.get("depends_on") or {}).get("nodes", [])), None)
        modelo = nodes.get(modelo_id, {}) if modelo_id else {}
        out.append({
            "unique_id": r["unique_id"], "status": r["status"], "failures": r.get("failures"),
            "message": r.get("message"), "teste": meta.get("name"),
            "coluna": kwargs.get("column_name") or node.get("column_name"),
            "valores": kwargs.get("values") or (kwargs.get("arguments") or {}).get("values"),
            "modelo": modelo.get("name"), "relacao": modelo.get("relation_name"),
        })
    return out


def erros_execucao(run_results):
    return [r["unique_id"] for r in run_results.get("results", []) if r.get("status") == "error"]
