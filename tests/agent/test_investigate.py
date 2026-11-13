import json
import unittest

from agent import investigate

DIAG = {
    "incidente_id": "inc1", "fonte": "tabelas_orfas", "source_table": "fato_pedidos",
    "classificacao": "AUSENTE_NA_ORIGEM", "ocorrencias": 160, "registros": 80, "pedidos": 49,
    "impacto_valor": 434200.62, "decisao_humana": True, "resumo": "Cabecalho nunca chegou a Bronze.",
    "proposta": "Solicitar ao ERP.", "evidencia": {"chaves": ["O00021", "O00042"], "carga_cabecalho": None},
}


class FakeDb:
    def __init__(self):
        self.consultas = []

    def read(self, sql, params=None):
        self.consultas.append((sql, params))
        return [{"order_id": "O00021", "item_seq": "1", "product_code": "P0037", "total_item": "1916.43"}]


def call(nome, args, i="c1"):
    return {"id": i, "type": "function", "function": {"name": nome, "arguments": json.dumps(args)}}


def pede(*calls):
    return {"role": "assistant", "content": None, "tool_calls": list(calls)}


def final(texto, perguntas=()):
    return {"role": "assistant", "content": json.dumps({"explicacao": texto, "perguntas_para_humano": list(perguntas)})}


class Roteiro:
    def __init__(self, respostas):
        self.respostas, self.chamadas = list(respostas), []

    def __call__(self, messages, tools=None):
        self.chamadas.append(tools is not None)
        return self.respostas.pop(0), {"prompt_tokens": 10, "completion_tokens": 5}


def rodar(respostas, max_llm=10, max_passos=4, diags=(DIAG,)):
    roteiro = Roteiro(respostas)
    f = investigate.Ferramentas(FakeDb(), "original", list(diags))
    out = investigate.investigar_todos(list(diags), roteiro, f, max_llm, max_passos)
    return out, roteiro


class TestInvestigacao(unittest.TestCase):
    def test_chamada_valida_e_numero_da_ferramenta(self):
        (narrs, calls, tin, tout, trace), _ = rodar([
            pede(call("amostrar_registros", {"incidente_id": "inc1", "limite": 2})),
            final("O primeiro item soma R$ 1.916,43."),
        ])
        self.assertEqual(narrs[0]["llm_status"], "OK")
        self.assertEqual([(t["ferramenta"], t["status"], t["linhas"]) for t in trace], [("amostrar_registros", "OK", 1)])
        self.assertEqual((calls, tin, tout), (2, 20, 10))

    def test_ferramenta_desconhecida_rejeitada(self):
        (narrs, _, _, _, trace), _ = rodar([pede(call("apagar_tabela", {})), final("Sem evidência adicional.")])
        self.assertEqual(trace[0]["status"], "REJEITADA")
        self.assertEqual(narrs[0]["llm_status"], "OK")

    def test_parametro_invalido_rejeitado(self):
        (_, _, _, _, trace), _ = rodar([
            pede(call("amostrar_registros", {"incidente_id": "inc1", "limite": 99})), final("Sem evidência adicional.")])
        self.assertEqual(trace[0]["status"], "REJEITADA")

    def test_parametro_extra_rejeitado(self):
        (_, _, _, _, trace), _ = rodar([
            pede(call("detalhar_incidente", {"incidente_id": "inc1", "sql": "DROP TABLE x"})), final("Sem evidência adicional.")])
        self.assertEqual(trace[0]["status"], "REJEITADA")

    def test_incidente_inexistente_rejeitado(self):
        (_, _, _, _, trace), _ = rodar([
            pede(call("detalhar_incidente", {"incidente_id": "outro"})), final("Sem evidência adicional.")])
        self.assertEqual(trace[0]["status"], "REJEITADA")

    def test_limite_de_passos_forca_resposta_sem_ferramentas(self):
        (narrs, _, _, _, trace), roteiro = rodar([
            pede(call("detalhar_incidente", {"incidente_id": "inc1"})),
            pede(call("detalhar_incidente", {"incidente_id": "inc1"}, "c2")),
            pede(call("detalhar_incidente", {"incidente_id": "inc1"}, "c3")),
        ], max_passos=2)
        self.assertEqual(len(trace), 2)
        self.assertEqual(roteiro.chamadas, [True, True, False])
        self.assertEqual(narrs[0]["llm_status"], "RESPOSTA_REJEITADA")

    def test_limite_dentro_de_uma_resposta(self):
        (_, _, _, _, trace), _ = rodar([
            pede(call("detalhar_incidente", {"incidente_id": "inc1"}, "a"),
                 call("detalhar_incidente", {"incidente_id": "inc1"}, "b"),
                 call("detalhar_incidente", {"incidente_id": "inc1"}, "c")),
            final("Sem evidência adicional."),
        ], max_passos=2)
        self.assertEqual([t["status"] for t in trace], ["OK", "OK", "REJEITADA"])

    def test_numero_inventado_rejeitado(self):
        (narrs, _, _, _, _), _ = rodar([final("Foram 999 pedidos afetados.")])
        self.assertEqual(narrs[0]["llm_status"], "RESPOSTA_REJEITADA")

    def test_indisponivel(self):
        f = investigate.Ferramentas(FakeDb(), "original", [DIAG])
        narrs, _, _, _, _ = investigate.investigar_todos([DIAG], investigate.chat_indisponivel, f, 5)
        self.assertEqual(narrs[0]["llm_status"], "INDISPONIVEL")
        self.assertEqual(narrs[0]["explicacao"], DIAG["resumo"])

    def test_orcamento_global(self):
        d2 = {**DIAG, "incidente_id": "inc2"}
        (narrs, calls, _, _, _), _ = rodar([final("Sem evidência adicional.")], max_llm=1, diags=(DIAG, d2))
        self.assertEqual([n["llm_status"] for n in narrs], ["OK", "ORCAMENTO_ESGOTADO"])
        self.assertEqual(calls, 1)


class TestAlerta(unittest.TestCase):
    def test_alerta_sem_numeros_da_evidencia(self):
        a = investigate.alerta(DIAG)
        self.assertEqual(set(a), {"incidente_id", "classificacao", "fonte", "source_table", "resumo", "proposta"})

    def test_numero_so_e_aceito_depois_da_ferramenta(self):
        (narrs, _, _, _, _), _ = rodar([final("Foram 49 pedidos afetados.")])
        self.assertEqual(narrs[0]["llm_status"], "RESPOSTA_REJEITADA")
        (narrs, _, _, _, _), _ = rodar([
            pede(call("detalhar_incidente", {"incidente_id": "inc1"})),
            final("Foram 49 pedidos afetados."),
        ])
        self.assertEqual(narrs[0]["llm_status"], "OK")


class TestDetalhar(unittest.TestCase):
    def test_resumo_com_ponteiros_sem_evidencia_bruta(self):
        d = {**DIAG, "fonte": "ingestion_log", "evidencia": {"source_file": "vendedores.csv", "status": "NOK"}}
        out = investigate.Ferramentas(FakeDb(), "original", [d]).executar("detalhar_incidente", {"incidente_id": "inc1"})
        self.assertNotIn("evidencia", out)
        self.assertEqual(out["arquivos_relacionados"], ["vendedores.csv"])

    def test_dominio_afetado_em_falha_dbt(self):
        ev = {"teste": "accepted_values", "modelo": "dim_regiao", "coluna": "regional_code"}
        d = {**DIAG, "fonte": "dbt", "evidencia": ev}
        out = investigate.Ferramentas(FakeDb(), "original", [d]).executar("detalhar_incidente", {"incidente_id": "inc1"})
        self.assertEqual(out["dominio_afetado"], {"modelo": "dim_regiao", "coluna": "regional_code"})


class TestLinhas(unittest.TestCase):
    def test_contagem_por_tipo_de_saida(self):
        self.assertEqual(investigate._linhas({"registros": 80, "pedidos": 49}), 1)
        self.assertEqual(investigate._linhas({"registros": [{"a": 1}, {"a": 2}]}), 2)
        self.assertEqual(investigate._linhas({"codigos_e_sinonimos": {"S": ["SUL"], "N": ["NORTE"]}}), 2)
        self.assertEqual(investigate._linhas({"erro": "x"}), 0)


class TestIsolamento(unittest.TestCase):
    def test_falha_inesperada_isolada_por_incidente(self):
        def chat_quebrado(messages, tools=None):
            raise RuntimeError("erro inesperado")
        d2 = {**DIAG, "incidente_id": "inc2"}
        f = investigate.Ferramentas(FakeDb(), "original", [DIAG, d2])
        narrs, _, _, _, _ = investigate.investigar_todos([DIAG, d2], chat_quebrado, f, 10)
        self.assertEqual([n["llm_status"] for n in narrs], ["ERRO", "ERRO"])
        self.assertEqual(narrs[1]["explicacao"], d2["resumo"])


class TestValidacao(unittest.TestCase):
    IDS = {"inc1"}

    def test_dominio(self):
        investigate.validar_args("dominio_valores", {"modelo": "dim_regiao", "coluna": "regional_code"}, self.IDS)
        with self.assertRaises(ValueError):
            investigate.validar_args("dominio_valores", {"modelo": "dim_x", "coluna": "y"}, self.IDS)

    def test_arquivo(self):
        investigate.validar_args("historico_cargas", {"source_file": "vendedores.csv"}, self.IDS)
        for ruim in ("../etc/passwd", "a' OR '1'='1", "", 5):
            with self.assertRaises(ValueError):
                investigate.validar_args("historico_cargas", {"source_file": ruim}, self.IDS)

    def test_limite_padrao(self):
        self.assertEqual(investigate.validar_args("amostrar_registros", {"incidente_id": "inc1"}, self.IDS)["limite"], 3)


if __name__ == "__main__":
    unittest.main()
