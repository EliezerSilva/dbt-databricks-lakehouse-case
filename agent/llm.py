from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"


class LlmIndisponivel(RuntimeError):
    pass


def databricks_llm(w, endpoint=ENDPOINT, max_tokens=400):
    def call(system, user):
        try:
            r = w.serving_endpoints.query(
                name=endpoint,
                messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
                          ChatMessage(role=ChatMessageRole.USER, content=user)],
                max_tokens=max_tokens, temperature=0.0)
        except Exception as e:  # noqa: BLE001 - qualquer falha do endpoint vira fallback
            raise LlmIndisponivel(str(e)[:300]) from e
        u = r.usage
        return (r.choices[0].message.content,
                (u.prompt_tokens or 0) if u else 0,
                (u.completion_tokens or 0) if u else 0)
    return call


def llm_indisponivel(system, user):
    raise LlmIndisponivel("falha simulada do endpoint")


def llm_resposta_invalida(system, user):
    return "resposta sem json", 0, 0
