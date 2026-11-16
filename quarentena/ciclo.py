import argparse
import json
import sys
import time

QUARENTENADO, ELEGIVEL, REPROCESSADO = "QUARENTENADO", "ELEGIVEL", "REPROCESSADO"
REJEITADO, EXCECAO_APROVADA = "REJEITADO", "EXCECAO_APROVADA"
TERMINAIS = (REJEITADO, EXCECAO_APROVADA)

PREFIXOS = ("dbt", "dbt_sim")

DDL_T = """CREATE TABLE IF NOT EXISTS workspace.{p}_quarentena.quarentena_eventos (
    seq BIGINT, entidade STRING, regra STRING, origem STRING, chave STRING, evento STRING,
    status_resultante STRING, ator STRING, nota STRING, criado_em TIMESTAMP
) COMMENT 'Ciclo de vida da quarentena (append-only) - o estado atual e o ultimo evento por entidade, regra e chave'"""

COLUNAS = ("seq", "entidade", "regra", "origem", "chave", "evento", "status_resultante", "ator", "nota")

ESTADO_T = """
SELECT entidade, regra, chave, origem, status_resultante AS status
FROM (
    SELECT *, row_number() OVER (PARTITION BY entidade, regra, chave ORDER BY seq DESC) AS rn
    FROM workspace.{p}_quarentena.quarentena_eventos
)
WHERE rn = 1
"""

ATUAIS_T = "SELECT entidade, regra, chave, origem FROM workspace.{p}_quarentena.quarentena_registros"

ALVO_T = """
SELECT 'pedidos_itens' AS entidade, concat(order_id, '-', cast(item_seq AS STRING)) AS chave FROM workspace.{p}_gold.fato_pedidos
UNION ALL
SELECT 'regioes' AS entidade, regional_code AS chave FROM workspace.{p}_gold.dim_regiao
"""

RESUMO_T = """
SELECT entidade, regra, status_resultante AS status, count(*) AS registros
FROM (
    SELECT *, row_number() OVER (PARTITION BY entidade, regra, chave ORDER BY seq DESC) AS rn
    FROM workspace.{p}_quarentena.quarentena_eventos
)
WHERE rn = 1
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""


def sql(prefixo="dbt"):
    if prefixo not in PREFIXOS:
        raise ValueError(f"prefixo invalido: {prefixo}")
    return {nome: t.format(p=prefixo) for nome, t in
            (("DDL", DDL_T), ("ESTADO", ESTADO_T), ("ATUAIS", ATUAIS_T), ("ALVO", ALVO_T), ("RESUMO", RESUMO_T))}


_PADRAO = sql("dbt")
DDL, ESTADO, ATUAIS, ALVO, RESUMO = (_PADRAO[n] for n in ("DDL", "ESTADO", "ATUAIS", "ALVO", "RESUMO"))


def _ev(chave, origem, evento, status, ator="sincronizacao", nota=None):
    entidade, regra, valor = chave
    return {"entidade": entidade, "regra": regra, "origem": origem, "chave": valor,
            "evento": evento, "status_resultante": status, "ator": ator, "nota": nota}


def transicoes(estado, atuais, alvo):
    """estado: {(entidade, regra, chave): {status, origem}}; atuais: {(entidade, regra, chave): origem};
    alvo: {(entidade, chave)} presentes na tabela de destino."""
    eventos = []
    for k, origem in sorted(atuais.items()):
        st = (estado.get(k) or {}).get("status")
        if st is None:
            eventos.append(_ev(k, origem, QUARENTENADO, QUARENTENADO))
        elif st in (ELEGIVEL, REPROCESSADO):
            eventos.append(_ev(k, origem, "REINCIDENTE", QUARENTENADO, nota=f"voltou a violar o contrato apos {st}"))
    for k, e in sorted(estado.items()):
        if k in atuais or e["status"] in TERMINAIS or e["status"] == REPROCESSADO:
            continue
        no_alvo = (k[0], k[2]) in alvo
        if e["status"] == QUARENTENADO:
            eventos.append(_ev(k, e["origem"], ELEGIVEL, ELEGIVEL, nota="nao viola mais a regra"))
        if no_alvo:
            eventos.append(_ev(k, e["origem"], REPROCESSADO, REPROCESSADO, nota="presente na tabela de destino"))
    return eventos


def acao_humana(estado, chave, acao, ator, nota):
    if not nota or not ator:
        raise ValueError("acao humana exige --ator e --nota")
    e = estado.get(chave)
    if e is None:
        raise ValueError("registro inexistente na quarentena")
    if acao == "rejeitar" and e["status"] in (QUARENTENADO, ELEGIVEL):
        return _ev(chave, e["origem"], REJEITADO, REJEITADO, ator, nota[:500])
    if acao == "aprovar-excecao" and e["status"] == QUARENTENADO:
        return _ev(chave, e["origem"], EXCECAO_APROVADA, EXCECAO_APROVADA, ator, nota[:500])
    raise ValueError(f"{acao} nao permitido a partir de {e['status']}")


def gravar(db, eventos, lote=10, prefixo="dbt"):
    q = sql(prefixo)
    db.write(q["DDL"])
    base = int(time.time() * 1000) * 1000
    for i in range(0, len(eventos), lote):
        parte, params, linhas = eventos[i:i + lote], {}, []
        for j, ev in enumerate(parte):
            ev = dict(ev, seq=(base + i + j, "BIGINT"))
            linhas.append("(" + ", ".join(f":{c}_{j}" for c in COLUNAS) + ", current_timestamp())")
            params.update({f"{c}_{j}": ev[c] for c in COLUNAS})
        db.write(f"INSERT INTO workspace.{prefixo}_quarentena.quarentena_eventos VALUES " + ", ".join(linhas), params)
    return len(eventos)


def carregar(db, prefixo="dbt"):
    q = sql(prefixo)
    db.write(q["DDL"])
    estado = {(r["entidade"], r["regra"], r["chave"]): {"status": r["status"], "origem": r["origem"]} for r in db.read(q["ESTADO"])}
    return estado


def main(argv=None):
    from agent.db import Db
    ap = argparse.ArgumentParser(description="Ciclo de vida da quarentena do contrato de dados")
    ap.add_argument("acao", choices=["sincronizar", "listar", "rejeitar", "aprovar-excecao"])
    ap.add_argument("--entidade")
    ap.add_argument("--regra")
    ap.add_argument("--chave")
    ap.add_argument("--ator")
    ap.add_argument("--nota")
    ap.add_argument("--prefixo", default="dbt", choices=PREFIXOS,
                    help="dbt = ambiente real; dbt_sim = simulacao de reprocessamento (scripts/simular_cabecalho_tardio.sh)")
    ap.add_argument("--profile", default="dbt-lakehouse-free")
    ap.add_argument("--warehouse", default="6ad31e4709e166fb")
    a = ap.parse_args(argv)
    db = Db(a.profile, a.warehouse)
    q = sql(a.prefixo)
    estado = carregar(db, a.prefixo)
    if a.acao == "listar":
        print(json.dumps(db.read(q["RESUMO"]), ensure_ascii=False, indent=2, default=str))
        return 0
    if a.acao == "sincronizar":
        atuais = {(r["entidade"], r["regra"], r["chave"]): r["origem"] for r in db.read(q["ATUAIS"])}
        alvo = {(r["entidade"], r["chave"]) for r in db.read(q["ALVO"])} if any(k not in atuais for k in estado) else set()
        eventos = transicoes(estado, atuais, alvo)
        n = gravar(db, eventos, prefixo=a.prefixo)
        resumo = {}
        for ev in eventos:
            resumo[ev["evento"]] = resumo.get(ev["evento"], 0) + 1
        print(json.dumps({"em_quarentena": len(atuais), "eventos_gravados": n, "por_evento": resumo}, ensure_ascii=False))
        return 0
    if not (a.entidade and a.regra and a.chave):
        sys.exit(f"{a.acao} exige --entidade, --regra e --chave")
    try:
        ev = acao_humana(estado, (a.entidade, a.regra, a.chave), a.acao, a.ator, a.nota)
    except ValueError as e:
        sys.exit(f"PARAR: {e}")
    gravar(db, [ev], prefixo=a.prefixo)
    print(f"{ev['evento']}: {a.entidade} {a.regra} {a.chave}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
