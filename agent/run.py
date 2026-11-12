import argparse
import datetime
import hashlib
import json
import uuid

from agent import classify, llm as llm_mod, narrate, tools
from agent.db import Db

DDL = [
    """CREATE TABLE IF NOT EXISTS workspace.agent.execucoes (
        agent_run_id STRING, iniciado_em TIMESTAMP, finalizado_em TIMESTAMP, modo STRING, status STRING,
        incidentes INT, sql_calls INT, llm_calls INT, erro STRING
    ) COMMENT 'Uma linha por execucao do agente de quarentena'""",
    """CREATE TABLE IF NOT EXISTS workspace.agent.diagnosticos (
        agent_run_id STRING, incidente_id STRING, fonte STRING, source_table STRING, classificacao STRING,
        ocorrencias BIGINT, registros BIGINT, pedidos BIGINT, impacto_valor DOUBLE, decisao_humana BOOLEAN,
        resumo STRING, proposta STRING, evidencia STRING, criado_em TIMESTAMP
    ) COMMENT 'Diagnosticos por incidente - o agente nao altera camadas de dados'""",
    """CREATE TABLE IF NOT EXISTS workspace.agent.narrativas (
        agent_run_id STRING, incidente_id STRING, llm_status STRING, modelo STRING, explicacao STRING,
        perguntas STRING, erro STRING, tokens_in BIGINT, tokens_out BIGINT, criado_em TIMESTAMP
    ) COMMENT 'Explicacao por incidente gerada por LLM e validada contra a evidencia'""",
]

INSERT_DIAG = """INSERT INTO workspace.agent.diagnosticos VALUES (
    :run_id, :incidente_id, :fonte, :source_table, :classificacao, :ocorrencias, :registros, :pedidos,
    :impacto_valor, :decisao_humana, :resumo, :proposta, :evidencia, current_timestamp())"""

INSERT_NARR = """INSERT INTO workspace.agent.narrativas VALUES (
    :run_id, :incidente_id, :llm_status, :modelo, :explicacao, :perguntas, :erro, :tokens_in, :tokens_out,
    current_timestamp())"""

INSERT_EXEC = """INSERT INTO workspace.agent.execucoes VALUES (
    :run_id, :iniciado_em, current_timestamp(), :modo, :status, :incidentes, :sql_calls, :llm_calls, :erro)"""


def _id(*parts):
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()[:16]


def diagnosticos_invalidos(db):
    out = []
    for r in tools.incidentes_invalidos(db):
        payload = json.loads(r["payload"])
        cls, resumo, humano = classify.classify_invalido(r["source_table"], r["motivo"], payload)
        out.append({
            "incidente_id": _id("tabelas_invalidas", r["source_table"], r["fp"]),
            "fonte": "tabelas_invalidas",
            "source_table": r["source_table"],
            "classificacao": cls,
            "ocorrencias": r["ocorrencias"],
            "registros": 1,
            "pedidos": None,
            "impacto_valor": None,
            "decisao_humana": humano,
            "resumo": resumo,
            "proposta": classify.PROPOSTAS[cls],
            "evidencia": {"motivo": r["motivo"], "payload": payload,
                          "primeira": r["primeira"], "ultima": r["ultima"]},
        })
    return out


def diagnosticos_orfaos(db):
    orfaos = tools.incidentes_orfaos(db)
    if not orfaos:
        return []
    presenca = {r["chave"]: r for r in tools.presenca_cabecalho(db)}
    carga = tools.carga_cabecalho(db, min(r["primeira"] for r in orfaos))
    carga_ok = None if carga is None else carga["status"] == "OK"
    grupos = {}
    for r in orfaos:
        if r["join_key_name"] != "order_id":
            cls, resumo, humano = (classify.NAO_CLASSIFICADO,
                                   f"Chave de orfao sem regra: {r['join_key_name']}", True)
        else:
            p = presenca.get(r["join_key_value"], {})
            cls, resumo, humano = classify.classify_orfao(
                bool(p.get("na_bronze")), bool(p.get("na_silver")), carga_ok)
        g = grupos.setdefault((r["source_table"], cls), {
            "resumo": resumo, "humano": humano, "chaves": [], "ocorrencias": 0, "registros": 0,
            "valor": 0.0, "primeira": r["primeira"], "ultima": r["ultima"]})
        g["chaves"].append(r["join_key_value"])
        g["ocorrencias"] += r["ocorrencias"]
        g["registros"] += r["registros"]
        g["valor"] += r["valor"] or 0.0
        g["primeira"] = min(g["primeira"], r["primeira"])
        g["ultima"] = max(g["ultima"], r["ultima"])
    out = []
    for (source_table, cls), g in sorted(grupos.items()):
        out.append({
            "incidente_id": _id("tabelas_orfas", source_table, cls),
            "fonte": "tabelas_orfas",
            "source_table": source_table,
            "classificacao": cls,
            "ocorrencias": g["ocorrencias"],
            "registros": g["registros"],
            "pedidos": len(g["chaves"]),
            "impacto_valor": round(g["valor"], 2),
            "decisao_humana": g["humano"],
            "resumo": g["resumo"],
            "proposta": classify.PROPOSTAS[cls],
            "evidencia": {"chaves": sorted(g["chaves"]), "carga_cabecalho": carga,
                          "primeira": g["primeira"], "ultima": g["ultima"]},
        })
    return out


def _para_tela(d):
    ev = dict(d["evidencia"])
    if "chaves" in ev:
        ev["chaves"] = ev["chaves"][:5] + ([f"... +{len(ev['chaves']) - 5}"] if len(ev["chaves"]) > 5 else [])
    return {**d, "evidencia": ev}


def gravar(db, run_id, inicio, modo, status, erro, diags, narrs, modelo, llm_calls):
    for ddl in DDL:
        db.write(ddl)
    for d in diags:
        db.write(INSERT_DIAG, {
            "run_id": run_id, "incidente_id": d["incidente_id"], "fonte": d["fonte"],
            "source_table": d["source_table"], "classificacao": d["classificacao"],
            "ocorrencias": (d["ocorrencias"], "BIGINT"), "registros": (d["registros"], "BIGINT"),
            "pedidos": (d["pedidos"], "BIGINT"), "impacto_valor": (d["impacto_valor"], "DOUBLE"),
            "decisao_humana": d["decisao_humana"], "resumo": d["resumo"], "proposta": d["proposta"],
            "evidencia": json.dumps(d["evidencia"], ensure_ascii=False, default=str),
        })
    for n in narrs:
        db.write(INSERT_NARR, {
            "run_id": run_id, "incidente_id": n["incidente_id"], "llm_status": n["llm_status"],
            "modelo": modelo, "explicacao": n["explicacao"],
            "perguntas": json.dumps(n["perguntas"], ensure_ascii=False), "erro": n["erro"],
            "tokens_in": (n["tokens_in"], "BIGINT"), "tokens_out": (n["tokens_out"], "BIGINT"),
        })
    db.write(INSERT_EXEC, {
        "run_id": run_id, "iniciado_em": (inicio, "TIMESTAMP"), "modo": modo,
        "status": status, "incidentes": (len(diags), "INT"), "sql_calls": (db.calls, "INT"),
        "llm_calls": (llm_calls, "INT"), "erro": erro,
    })


def main(argv=None):
    ap = argparse.ArgumentParser(description="Agente de quarentena")
    ap.add_argument("--profile", default="dbt-lakehouse-free")
    ap.add_argument("--warehouse", default="6ad31e4709e166fb")
    ap.add_argument("--endpoint", default=llm_mod.ENDPOINT)
    ap.add_argument("--sem-llm", action="store_true")
    ap.add_argument("--simular-falha", choices=["INDISPONIVEL", "RESPOSTA_INVALIDA"])
    ap.add_argument("--max-llm-calls", type=int, default=4)
    ap.add_argument("--gravar", action="store_true", help="grava em workspace.agent; sem a flag apenas imprime")
    a = ap.parse_args(argv)
    db = Db(a.profile, a.warehouse)
    if a.sem_llm:
        llm, modo = None, "deterministico"
    elif a.simular_falha == "INDISPONIVEL":
        llm, modo = llm_mod.llm_indisponivel, "simulado_indisponivel"
    elif a.simular_falha == "RESPOSTA_INVALIDA":
        llm, modo = llm_mod.llm_resposta_invalida, "simulado_resposta_invalida"
    else:
        llm, modo = llm_mod.databricks_llm(db.w, a.endpoint), "llm"
    run_id = uuid.uuid4().hex
    inicio = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    status, erro, diags, narrs, calls, tin, tout = "OK", None, [], [], 0, 0, 0
    try:
        diags = diagnosticos_invalidos(db) + diagnosticos_orfaos(db)
        narrs, calls, tin, tout = narrate.narrar(diags, llm, a.max_llm_calls)
    except Exception as e:  # noqa: BLE001 - registrado na execucao
        status, erro = "ERRO", str(e)[:1000]
    print(json.dumps({"agent_run_id": run_id, "modo": modo, "status": status, "erro": erro,
                      "sql_calls": db.calls, "llm_calls": calls, "tokens_in": tin, "tokens_out": tout,
                      "diagnosticos": [_para_tela(d) for d in diags], "narrativas": narrs},
                     ensure_ascii=False, indent=2, default=str))
    if a.gravar:
        gravar(db, run_id, inicio, modo, status, erro, diags, narrs, a.endpoint if llm else None, calls)
        print(f"gravado: agent_run_id={run_id}")
    return 0 if status == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
