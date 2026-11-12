import json
import re

from agent.llm import LlmIndisponivel

SYSTEM = (
    "Voce e um analista de qualidade de dados de um Lakehouse. Recebe um diagnostico ja classificado "
    "por regras deterministicas e a evidencia coletada. Explique em portugues do Brasil, em no maximo "
    "4 frases, o que aconteceu, por que a classificacao se sustenta e qual o impacto. Nao altere a "
    "classificacao nem a proposta. Use somente fatos presentes na evidencia e cite numeros exatamente "
    "como aparecem nela. Responda apenas com JSON valido no formato "
    '{"explicacao": "...", "perguntas_para_humano": ["..."]}'
)

_NUM = re.compile(r"\d[\d.,]*\d|\d")


def _to_float(tok):
    t = tok
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".") if t.rfind(",") > t.rfind(".") else t.replace(",", "")
    elif "," in t:
        t = t.replace(",", "") if re.fullmatch(r"\d{1,3}(,\d{3})+", t) else t.replace(",", ".")
    elif re.fullmatch(r"\d{1,3}(\.\d{3})+", t):
        t = t.replace(".", "")
    try:
        return float(t)
    except ValueError:
        return None


def numbers_in(text):
    return {round(x, 2) for x in (_to_float(t) for t in _NUM.findall(text or "")) if x is not None}


def parse(content):
    s = (content or "").strip()
    i, j = s.find("{"), s.rfind("}")
    if i < 0 or j <= i:
        raise ValueError("resposta sem JSON")
    d = json.loads(s[i:j + 1])
    exp = d.get("explicacao")
    if not isinstance(exp, str) or not exp.strip():
        raise ValueError("explicacao ausente")
    perg = d.get("perguntas_para_humano") or []
    if not isinstance(perg, list) or not all(isinstance(p, str) for p in perg):
        raise ValueError("perguntas_para_humano invalido")
    return exp.strip(), [p.strip() for p in perg][:3]


def validar(explicacao, perguntas, contexto_txt):
    extra = numbers_in(explicacao + " " + " ".join(perguntas)) - numbers_in(contexto_txt)
    if extra:
        raise ValueError(f"numeros sem suporte na evidencia: {sorted(extra)[:5]}")


def contexto(d):
    ev = dict(d["evidencia"])
    if "chaves" in ev:
        ev["chaves_amostra"] = ev.pop("chaves")[:5]
    campos = ("classificacao", "source_table", "ocorrencias", "registros", "pedidos",
              "impacto_valor", "resumo", "proposta")
    return {**{k: d[k] for k in campos}, "evidencia": ev}


def narrar(diags, llm, max_calls):
    out, calls, tin, tout = [], 0, 0, 0
    for d in diags:
        base = {"incidente_id": d["incidente_id"], "explicacao": d["resumo"], "perguntas": [],
                "erro": None, "tokens_in": 0, "tokens_out": 0}
        if llm is None:
            out.append({**base, "llm_status": "DESATIVADO"})
            continue
        if calls >= max_calls:
            out.append({**base, "llm_status": "ORCAMENTO_ESGOTADO"})
            continue
        ctx = json.dumps(contexto(d), ensure_ascii=False, default=str)
        calls += 1
        try:
            content, ti, to = llm(SYSTEM, ctx)
        except LlmIndisponivel as e:
            out.append({**base, "llm_status": "INDISPONIVEL", "erro": str(e)[:300]})
            continue
        tin, tout = tin + ti, tout + to
        try:
            exp, perg = parse(content)
            validar(exp, perg, ctx)
        except ValueError as e:
            out.append({**base, "llm_status": "RESPOSTA_REJEITADA", "erro": str(e)[:300],
                        "tokens_in": ti, "tokens_out": to})
            continue
        out.append({**base, "llm_status": "OK", "explicacao": exp, "perguntas": perg,
                    "tokens_in": ti, "tokens_out": to})
    return out, calls, tin, tout
