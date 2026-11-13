import argparse
import json
import re
import sys
from functools import lru_cache
from pathlib import Path

import yaml

ARQUIVO = Path(__file__).resolve().parent.parent / "ontologia" / "ontologia.yml"
SCHEMA_GOLD = "workspace.dbt_gold"
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


@lru_cache(maxsize=1)
def carregar():
    return yaml.safe_load(ARQUIVO.read_text(encoding="utf-8"))


def entidade(nome):
    return carregar()["entidades"][nome]


def relacao(de, para, o=None):
    for r in (o or carregar())["relacoes"]:
        if r["de"] == de and r["para"] == para:
            return r
    raise KeyError(f"relacao inexistente: {de} -> {para}")


def atribuicoes():
    return carregar()["atribuicao"]


def dominios():
    return {(d["modelo"], d["coluna"]): d["valores"] for d in carregar()["dominios"]}


def cadeia(dimensao):
    a = atribuicoes()[dimensao]
    cam = a["caminho"]
    passos = [cam[0]] + [f"{para} (via {relacao(de, para)['via']})" for de, para in zip(cam, cam[1:])]
    if a.get("atributo"):
        passos[-1] += f", atributo {a['atributo']}"
    return " -> ".join(passos)


def sql_causa(dimensao):
    a = atribuicoes()[dimensao]
    cam = a["caminho"]
    if a.get("atributo"):
        e = entidade(cam[-1])
        return (f"SELECT cast({e['chave']} AS STRING) AS chave, cast({a['atributo']} AS STRING) AS valor\n"
                f"FROM {SCHEMA_GOLD}.{e['modelo']}\nWHERE {a['atributo']} IS NULL\nORDER BY 1\nLIMIT 20")
    x, y = entidade(cam[-2]), entidade(cam[-1])
    via = relacao(cam[-2], cam[-1])["via"]
    return (f"SELECT cast(x.{x['chave']} AS STRING) AS chave, cast(x.{via} AS STRING) AS valor\n"
            f"FROM {SCHEMA_GOLD}.{x['modelo']} x\n"
            f"LEFT JOIN {SCHEMA_GOLD}.{y['modelo']} y ON x.{via} = y.{y['chave']}\n"
            f"WHERE y.{y['chave']} IS NULL\nORDER BY 1\nLIMIT 20")


def validar_interno(o=None):
    o = o or carregar()
    erros = []

    def ident(v, ctx):
        for x in (v if isinstance(v, list) else [v]):
            if not isinstance(x, str) or not _IDENT.match(x):
                erros.append(f"{ctx}: identificador invalido {x!r}")

    ents = o.get("entidades") or {}
    for n, e in ents.items():
        ident(n, "entidade")
        ident(e.get("modelo"), f"{n}.modelo")
        ident(e.get("chave"), f"{n}.chave")
        for d in e.get("dimensoes_semanticas", []):
            ident(d, f"{n}.dimensoes_semanticas")
    pares = set()
    for r in o.get("relacoes") or []:
        if r.get("de") not in ents or r.get("para") not in ents:
            erros.append(f"relacao com entidade inexistente: {r.get('de')} -> {r.get('para')}")
        ident(r.get("via"), f"relacao {r.get('de')} -> {r.get('para')}.via")
        pares.add((r.get("de"), r.get("para")))
    dims = {d for e in ents.values() for d in e.get("dimensoes_semanticas", [])}
    for dim, a in (o.get("atribuicao") or {}).items():
        if dim not in dims:
            erros.append(f"atribuicao de dimensao sem entidade: {dim}")
        cam = a.get("caminho") or []
        for de, para in zip(cam, cam[1:]):
            if (de, para) not in pares:
                erros.append(f"atribuicao {dim}: relacao ausente {de} -> {para}")
        if a.get("atributo"):
            ident(a["atributo"], f"atribuicao {dim}.atributo")
        if a.get("genero") not in ("m", "f") or not a.get("nome") or not a.get("rotulo_lacuna"):
            erros.append(f"atribuicao {dim}: nome, genero (m/f) e rotulo_lacuna sao obrigatorios")
    modelos = {e.get("modelo") for e in ents.values()}
    for d in o.get("dominios") or []:
        if d.get("modelo") not in modelos:
            erros.append(f"dominio em modelo fora da ontologia: {d.get('modelo')}")
    for m, d in (o.get("metricas") or {}).items():
        if d.get("grao") not in ents:
            erros.append(f"metrica {m}: grao inexistente {d.get('grao')}")
    return erros


def validar_manifest(manifest, o=None):
    o = o or carregar()
    erros = []
    modelos = {n["name"]: n for n in manifest.get("nodes", {}).values() if n.get("resource_type") == "model"}

    def declarado(mv, nome):
        raw = (modelos.get(mv) or {}).get("raw_code") or ""
        return re.search(rf"^\s*-\s*name:\s*{re.escape(nome)}\s*$", raw, re.M) is not None

    for n, e in o["entidades"].items():
        if e["modelo"] not in modelos:
            erros.append(f"entidade {n}: modelo {e['modelo']} ausente no manifest")
    mv_dims = o["camada_semantica"]["metric_view_dimensoes"]
    for e in o["entidades"].values():
        for d in e.get("dimensoes_semanticas", []):
            if not declarado(mv_dims, d):
                erros.append(f"dimensao {d} ausente na metric view {mv_dims}")
    for m, d in o["metricas"].items():
        if not declarado(d["metric_view"], m):
            erros.append(f"metrica {m} ausente na metric view {d['metric_view']}")
    return erros


def main(argv=None):
    ap = argparse.ArgumentParser(description="Valida a ontologia (interna e contra o manifest do dbt)")
    ap.add_argument("--manifest")
    a = ap.parse_args(argv)
    erros = validar_interno()
    if a.manifest:
        erros += validar_manifest(json.loads(Path(a.manifest).read_text()))
    for e in erros:
        print(f"ERRO: {e}")
    print(f"ontologia: {len(carregar()['entidades'])} entidades, {len(carregar()['relacoes'])} relacoes, "
          f"{len(erros)} erro(s)")
    return 1 if erros else 0


if __name__ == "__main__":
    sys.exit(main())
