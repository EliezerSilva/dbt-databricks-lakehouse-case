import json
import re
import time

from agent import dominios, narrate, tools
from agent.llm import LlmIndisponivel

MAX_PASSOS = 4

SYSTEM = (
    "Você investiga um incidente de qualidade de dados seguindo o procedimento: 1) detalhar_incidente; "
    "2) verificar na fonte com amostrar_registros; 3) se houver arquivos relacionados, historico_cargas; "
    "4) se houver domínio afetado, dominio_valores. Pule os passos que não se aplicam; no máximo 4 chamadas. "
    "Use o incidente_id recebido. Se o alerta indicar reincidência, mencione-a na explicação. "
    "A classificação recebida é definitiva e não pode ser alterada. O contexto inicial é só o alerta: "
    "números, registros e histórico precisam ser obtidos com as ferramentas antes de serem citados. " + narrate.SYSTEM
)

_MODELOS = sorted({m for m, _ in dominios.DOMINIOS})
_COLUNAS = sorted({c for _, c in dominios.DOMINIOS})

TOOLS = [
    {"type": "function", "function": {
        "name": "detalhar_incidente",
        "description": "Resumo do incidente: classificação, contagens, impacto, proposta e evidência principal.",
        "parameters": {"type": "object", "properties": {"incidente_id": {"type": "string"}},
                       "required": ["incidente_id"]}}},
    {"type": "function", "function": {
        "name": "amostrar_registros",
        "description": "Amostra de até 5 registros afetados pelo incidente.",
        "parameters": {"type": "object", "properties": {
            "incidente_id": {"type": "string"},
            "limite": {"type": "integer", "minimum": 1, "maximum": 5}},
            "required": ["incidente_id"]}}},
    {"type": "function", "function": {
        "name": "historico_cargas",
        "description": "Últimas 5 cargas registradas para um arquivo de origem (status, linhas, layout detectado).",
        "parameters": {"type": "object", "properties": {"source_file": {"type": "string"}},
                       "required": ["source_file"]}}},
    {"type": "function", "function": {
        "name": "dominio_valores",
        "description": "Códigos válidos e sinônimos conhecidos de um domínio de dados.",
        "parameters": {"type": "object", "properties": {
            "modelo": {"type": "string", "enum": _MODELOS},
            "coluna": {"type": "string", "enum": _COLUNAS}},
            "required": ["modelo", "coluna"]}}},
]

_PARAMS = {
    "detalhar_incidente": {"incidente_id"},
    "amostrar_registros": {"incidente_id", "limite"},
    "historico_cargas": {"source_file"},
    "dominio_valores": {"modelo", "coluna"},
}
_ARQUIVO = re.compile(r"^[A-Za-z0-9_.\-]{1,80}$")

AMOSTRA_ORFAOS = """
SELECT join_key_value AS order_id,
       get_json_object(payload, '$.item_seq') AS item_seq,
       get_json_object(payload, '$.product_code') AS product_code,
       get_json_object(payload, '$.total_item') AS total_item
FROM {silver}.tabelas_orfas
WHERE array_contains(from_json(:chaves, 'array<string>'), join_key_value)
GROUP BY ALL
ORDER BY 1, 2
LIMIT 5
"""

HISTORICO = """
SELECT source_file, target_table, status, records_loaded, detected_schema, expected_schema,
       cast(pipeline_execution_timestamp AS STRING) AS executado_em
FROM {control}.ingestion_log
WHERE source_file = :arquivo
ORDER BY pipeline_execution_timestamp DESC
LIMIT 5
"""


class OrcamentoEsgotado(Exception):
    pass


class Orcamento:
    def __init__(self, maximo):
        self.maximo, self.usadas = maximo, 0

    def consumir(self):
        if self.usadas >= self.maximo:
            raise OrcamentoEsgotado()
        self.usadas += 1


def validar_args(nome, args, ids_validos):
    if nome not in _PARAMS:
        raise ValueError(f"ferramenta desconhecida: {nome}")
    if not isinstance(args, dict):
        raise ValueError("argumentos devem ser um objeto JSON")
    extras = set(args) - _PARAMS[nome]
    if extras:
        raise ValueError(f"parametros nao permitidos: {sorted(extras)}")
    if "incidente_id" in _PARAMS[nome] and args.get("incidente_id") not in ids_validos:
        raise ValueError("incidente_id inexistente nesta execucao")
    if nome == "amostrar_registros":
        lim = args.get("limite", 3)
        if isinstance(lim, bool) or not isinstance(lim, int) or not 1 <= lim <= 5:
            raise ValueError("limite deve ser inteiro entre 1 e 5")
        args = {**args, "limite": lim}
    if nome == "historico_cargas":
        arq = args.get("source_file")
        if not isinstance(arq, str) or not _ARQUIVO.match(arq):
            raise ValueError("source_file invalido")
    if nome == "dominio_valores" and (args.get("modelo"), args.get("coluna")) not in dominios.DOMINIOS:
        raise ValueError("dominio desconhecido")
    return args


class Ferramentas:
    def __init__(self, db, amb, diags):
        self.db, self.amb = db, amb
        self.diags = {d["incidente_id"]: d for d in diags}

    def ids(self):
        return set(self.diags)

    def executar(self, nome, args):
        return getattr(self, "_" + nome)(**args)

    def _detalhar_incidente(self, incidente_id):
        d = self.diags[incidente_id]
        ev = d["evidencia"]
        campos = ("classificacao", "fonte", "source_table", "ocorrencias", "registros", "pedidos",
                  "impacto_valor", "resumo", "proposta")
        out = {k: d[k] for k in campos}
        out["arquivos_relacionados"] = sorted(_arquivos(ev))
        if ev.get("teste") == "accepted_values" and ev.get("modelo") and ev.get("coluna"):
            out["dominio_afetado"] = {"modelo": ev["modelo"], "coluna": ev["coluna"]}
        out["aprofundar"] = "amostrar_registros para os registros; historico_cargas para as cargas dos arquivos relacionados"
        return out

    def _amostrar_registros(self, incidente_id, limite):
        d = self.diags[incidente_id]
        ev = d["evidencia"]
        if d["fonte"] == "tabelas_orfas":
            chaves = ev["chaves"][:limite]
            rows = self.db.read(tools.sql(AMOSTRA_ORFAOS, self.amb), {"chaves": json.dumps(chaves)})
            return {"registros": rows[:limite]}
        if d["fonte"] == "dbt":
            return {"registros": (ev.get("valores_fora") or [])[:limite]}
        if d["fonte"] == "tabelas_invalidas":
            return {"registros": [ev.get("payload")]}
        return {"registros": [ev]}

    def _historico_cargas(self, source_file):
        return {"cargas": self.db.read(tools.sql(HISTORICO, self.amb), {"arquivo": source_file})}

    def _dominio_valores(self, modelo, coluna):
        return {"codigos_e_sinonimos": dominios.DOMINIOS[(modelo, coluna)]}


def _linhas(saida):
    if not isinstance(saida, dict) or "erro" in saida:
        return 0
    for k in ("registros", "cargas", "codigos_e_sinonimos"):
        v = saida.get(k)
        if isinstance(v, (list, dict)):
            return len(v)
    return 1 if saida else 0


def _arquivos(obj):
    achados = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "source_file" and isinstance(v, str):
                achados.add(v)
            else:
                achados |= _arquivos(v)
    elif isinstance(obj, list):
        for v in obj:
            achados |= _arquivos(v)
    return achados


def alerta(diag):
    campos = ("incidente_id", "classificacao", "fonte", "source_table", "resumo", "proposta")
    out = {k: diag[k] for k in campos}
    est = diag.get("estado") or {}
    if est.get("reincidente"):
        out["historico"] = {"reincidente": True, "reaberturas": est.get("reaberturas"),
                            "deteccoes_anteriores": est.get("deteccoes_anteriores")}
    return out


def investigar(diag, chat, ferramentas, orcamento, max_passos=MAX_PASSOS):
    ctx = json.dumps(alerta(diag), ensure_ascii=False, default=str)
    msgs = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": ctx}]
    evidencias, trace, usadas = [ctx], [], 0
    r = {"incidente_id": diag["incidente_id"], "explicacao": diag["resumo"], "perguntas": [],
         "erro": None, "tokens_in": 0, "tokens_out": 0}
    while True:
        try:
            orcamento.consumir()
        except OrcamentoEsgotado:
            return {**r, "llm_status": "ORCAMENTO_ESGOTADO"}, trace
        try:
            msg, usage = chat(msgs, TOOLS if usadas < max_passos else None)
        except LlmIndisponivel as e:
            return {**r, "llm_status": "INDISPONIVEL", "erro": str(e)[:300]}, trace
        r["tokens_in"] += usage.get("prompt_tokens") or 0
        r["tokens_out"] += usage.get("completion_tokens") or 0
        calls = msg.get("tool_calls") or []
        if calls and usadas < max_passos:
            msgs.append({"role": "assistant", "content": msg.get("content"), "tool_calls": calls})
            for c in calls:
                usadas += 1
                fn = c.get("function") or {}
                nome, bruto = fn.get("name"), fn.get("arguments") or "{}"
                t0 = time.monotonic()
                if usadas > max_passos:
                    saida, status = {"erro": "limite de chamadas de ferramenta atingido"}, "REJEITADA"
                else:
                    try:
                        args = validar_args(nome, json.loads(bruto), ferramentas.ids())
                        saida, status = ferramentas.executar(nome, args), "OK"
                    except ValueError as e:
                        saida, status = {"erro": str(e)[:200]}, "REJEITADA"
                    except Exception:  # noqa: BLE001 - falha de execucao vira resultado da ferramenta
                        saida, status = {"erro": "falha ao executar a ferramenta"}, "ERRO"
                txt = json.dumps(saida, ensure_ascii=False, default=str)[:2000]
                evidencias.append(txt)
                trace.append({"incidente_id": diag["incidente_id"], "passo": usadas, "ferramenta": nome,
                              "parametros": str(bruto)[:500], "status": status, "linhas": _linhas(saida),
                              "duracao_ms": int((time.monotonic() - t0) * 1000)})
                msgs.append({"role": "tool", "tool_call_id": c.get("id"), "content": txt})
            continue
        try:
            exp, perg = narrate.parse(msg.get("content"))
            narrate.validar(exp, perg, " ".join(evidencias))
        except ValueError as e:
            return {**r, "llm_status": "RESPOSTA_REJEITADA", "erro": str(e)[:300]}, trace
        return {**r, "llm_status": "OK", "explicacao": exp, "perguntas": perg}, trace


def investigar_todos(diags, chat, ferramentas, max_llm_calls, max_passos=MAX_PASSOS):
    orcamento = Orcamento(max_llm_calls)
    narrs, trace = [], []
    for d in diags:
        try:
            n, t = investigar(d, chat, ferramentas, orcamento, max_passos)
        except Exception as e:  # noqa: BLE001 - falha isolada no incidente; os demais seguem
            n = {"incidente_id": d["incidente_id"], "explicacao": d["resumo"], "perguntas": [],
                 "erro": f"falha interna: {str(e)[:200]}", "tokens_in": 0, "tokens_out": 0, "llm_status": "ERRO"}
            t = []
        narrs.append(n)
        trace.extend(t)
    return (narrs, orcamento.usadas, sum(n["tokens_in"] for n in narrs),
            sum(n["tokens_out"] for n in narrs), trace)


def chat_indisponivel(messages, tools=None):
    raise LlmIndisponivel("falha simulada do endpoint")


def chat_resposta_invalida(messages, tools=None):
    return {"role": "assistant", "content": "resposta sem json"}, {}
