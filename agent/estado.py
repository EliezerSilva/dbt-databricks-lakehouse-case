import argparse
import json
import sys
import time

ABERTO, RECONHECIDO, RESOLVIDO = "ABERTO", "RECONHECIDO", "RESOLVIDO"

DDL = """CREATE TABLE IF NOT EXISTS workspace.agent.incidentes_eventos (
    seq BIGINT, ambiente STRING, incidente_id STRING, fonte STRING, evento STRING, status_resultante STRING,
    classificacao STRING, source_table STRING, ator STRING, agent_run_id STRING, nota STRING, criado_em TIMESTAMP
) COMMENT 'Ciclo de vida dos incidentes (append-only) - o estado atual e o ultimo evento'"""

INSERT = """INSERT INTO workspace.agent.incidentes_eventos VALUES (
    :seq, :ambiente, :incidente_id, :fonte, :evento, :status_resultante, :classificacao, :source_table,
    :ator, :agent_run_id, :nota, current_timestamp())"""

ESTADO = """
SELECT incidente_id, fonte, status_resultante AS status, classificacao, source_table, nota,
       deteccoes, reaberturas, cast(primeira AS STRING) AS primeira, cast(ultima AS STRING) AS ultima
FROM (
    SELECT *,
           row_number() OVER (PARTITION BY incidente_id ORDER BY seq DESC) AS rn,
           count(CASE WHEN evento IN ('NOVO', 'DETECTADO', 'REABERTO') THEN 1 END) OVER (PARTITION BY incidente_id) AS deteccoes,
           count(CASE WHEN evento = 'REABERTO' THEN 1 END) OVER (PARTITION BY incidente_id) AS reaberturas,
           min(criado_em) OVER (PARTITION BY incidente_id) AS primeira,
           max(criado_em) OVER (PARTITION BY incidente_id) AS ultima
    FROM workspace.agent.incidentes_eventos
    WHERE ambiente = :ambiente
)
WHERE rn = 1
ORDER BY incidente_id
"""


def carregar(db, ambiente):
    return {r["incidente_id"]: r for r in db.read(ESTADO, {"ambiente": ambiente})}


def _evento(iid, fonte, evento, status, classificacao, source_table, nota=None, ator="agente"):
    return {"incidente_id": iid, "fonte": fonte, "evento": evento, "status_resultante": status,
            "classificacao": classificacao, "source_table": source_table, "ator": ator, "nota": nota}


def transicoes(estado_atual, diags, fontes_avaliadas):
    eventos, atuais = [], {d["incidente_id"]: d for d in diags}
    for iid, d in atuais.items():
        e = estado_atual.get(iid)
        if e is None:
            ev, st, nota = "NOVO", ABERTO, None
        elif e["status"] == RESOLVIDO:
            ev, st, nota = "REABERTO", ABERTO, None
        elif e["status"] == RECONHECIDO:
            ev, st, nota = "DETECTADO", RECONHECIDO, e.get("nota")
        else:
            ev, st, nota = "DETECTADO", ABERTO, None
        eventos.append(_evento(iid, d["fonte"], ev, st, d["classificacao"], d["source_table"], nota))
    for iid, e in estado_atual.items():
        if iid not in atuais and e["status"] in (ABERTO, RECONHECIDO) and e.get("fonte") in fontes_avaliadas:
            eventos.append(_evento(iid, e.get("fonte"), "RESOLVIDO", RESOLVIDO, e.get("classificacao"),
                                   e.get("source_table"), "nao detectado nesta execucao"))
    return eventos


def anotar(diags, estado_atual, eventos):
    por_id = {e["incidente_id"]: e for e in eventos}
    for d in diags:
        ant = estado_atual.get(d["incidente_id"]) or {}
        ev = por_id.get(d["incidente_id"])
        reab = int(ant.get("reaberturas") or 0) + (1 if ev and ev["evento"] == "REABERTO" else 0)
        d["estado"] = {
            "status": ev["status_resultante"] if ev else ABERTO,
            "evento": ev["evento"] if ev else "SEM_ESTADO",
            "deteccoes_anteriores": int(ant.get("deteccoes") or 0),
            "reaberturas": reab,
            "reincidente": reab > 0,
            "nota": ev["nota"] if ev else None,
        }


def narrativa_suprimida(d):
    nota = d["estado"].get("nota")
    return {"incidente_id": d["incidente_id"], "llm_status": "SUPRIMIDO_RECONHECIDO",
            "explicacao": d["resumo"] + (f" Reconhecido: {nota}" if nota else ""),
            "perguntas": [], "erro": None, "tokens_in": 0, "tokens_out": 0}


def marcar_reincidencia(narrs, diags):
    por_id = {d["incidente_id"]: d for d in diags}
    for n in narrs:
        est = (por_id.get(n["incidente_id"]) or {}).get("estado") or {}
        if est.get("reincidente") and "reincid" not in (n.get("explicacao") or "").lower():
            n["explicacao"] = f"Incidente reincidente (reaberto {est.get('reaberturas') or 1} vez(es)). " + (n.get("explicacao") or "")


def gravar_eventos(db, ambiente, eventos, agent_run_id):
    db.write(DDL)
    base = int(time.time() * 1000) * 1000
    for i, ev in enumerate(eventos):
        db.write(INSERT, {
            "seq": (base + i, "BIGINT"), "ambiente": ambiente, "incidente_id": ev["incidente_id"],
            "fonte": ev["fonte"], "evento": ev["evento"], "status_resultante": ev["status_resultante"],
            "classificacao": ev["classificacao"], "source_table": ev["source_table"], "ator": ev["ator"],
            "agent_run_id": agent_run_id, "nota": ev["nota"],
        })


def main(argv=None):
    from agent.db import Db
    ap = argparse.ArgumentParser(description="Estado dos incidentes do agente de quarentena")
    ap.add_argument("acao", choices=["listar", "reconhecer"])
    ap.add_argument("--ambiente", required=True, choices=["original", "dev", "cenarios"])
    ap.add_argument("--incidente")
    ap.add_argument("--nota")
    ap.add_argument("--profile", default="dbt-lakehouse-free")
    ap.add_argument("--warehouse", default="6ad31e4709e166fb")
    a = ap.parse_args(argv)
    db = Db(a.profile, a.warehouse)
    db.write(DDL)
    atual = carregar(db, a.ambiente)
    if a.acao == "listar":
        print(json.dumps(list(atual.values()), ensure_ascii=False, indent=2, default=str))
        return 0
    if not a.incidente or not a.nota:
        sys.exit("reconhecer exige --incidente e --nota")
    e = atual.get(a.incidente)
    if e is None or e["status"] != ABERTO:
        sys.exit(f"incidente inexistente ou nao ABERTO: {e['status'] if e else 'inexistente'}")
    ev = _evento(a.incidente, e["fonte"], RECONHECIDO, RECONHECIDO, e["classificacao"], e["source_table"],
                 a.nota[:500], ator="humano")
    gravar_eventos(db, a.ambiente, [ev], None)
    print(f"reconhecido: {a.incidente}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
