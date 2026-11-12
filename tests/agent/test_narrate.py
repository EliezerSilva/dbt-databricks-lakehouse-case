import json
import unittest

from agent import narrate
from agent.llm import llm_indisponivel, llm_resposta_invalida

DIAG = {
    "incidente_id": "abc", "classificacao": "AUSENTE_NA_ORIGEM", "source_table": "fato_pedidos",
    "ocorrencias": 160, "registros": 80, "pedidos": 49, "impacto_valor": 434200.62,
    "resumo": "Cabecalho nunca chegou a Bronze.", "proposta": "Solicitar ao ERP.",
    "evidencia": {"chaves": ["O00021", "O00042"], "carga_cabecalho": {"records_loaded": 403}},
}


def llm_fixo(texto):
    def call(system, user):
        return json.dumps({"explicacao": texto, "perguntas_para_humano": ["Quem responde pelo ERP?"]}), 10, 5
    return call


class TestNumeros(unittest.TestCase):
    def test_formatos_equivalentes(self):
        self.assertEqual(narrate.numbers_in("R$ 434.200,62 em 49 pedidos"), {434200.62, 49.0})
        self.assertEqual(narrate.numbers_in("434200.62"), {434200.62})
        self.assertEqual(narrate.numbers_in("434,200.62"), {434200.62})

    def test_numero_inventado_rejeitado(self):
        ctx = json.dumps(narrate.contexto(DIAG))
        with self.assertRaises(ValueError):
            narrate.validar("Foram 50 pedidos afetados.", [], ctx)
        narrate.validar("Foram 49 pedidos e R$ 434.200,62 fora da Gold.", [], ctx)


class TestNarrar(unittest.TestCase):
    def test_ok(self):
        out, calls, tin, tout = narrate.narrar([DIAG], llm_fixo("Os 49 pedidos somam R$ 434.200,62."), 4)
        self.assertEqual(out[0]["llm_status"], "OK")
        self.assertEqual((calls, tin, tout), (1, 10, 5))

    def test_numero_sem_suporte_mantem_resumo(self):
        out, _, _, _ = narrate.narrar([DIAG], llm_fixo("Foram 51 pedidos."), 4)
        self.assertEqual(out[0]["llm_status"], "RESPOSTA_REJEITADA")
        self.assertEqual(out[0]["explicacao"], DIAG["resumo"])

    def test_desativado(self):
        out, calls, _, _ = narrate.narrar([DIAG], None, 4)
        self.assertEqual((out[0]["llm_status"], calls), ("DESATIVADO", 0))

    def test_indisponivel(self):
        out, _, _, _ = narrate.narrar([DIAG], llm_indisponivel, 4)
        self.assertEqual(out[0]["llm_status"], "INDISPONIVEL")

    def test_resposta_invalida(self):
        out, _, _, _ = narrate.narrar([DIAG], llm_resposta_invalida, 4)
        self.assertEqual(out[0]["llm_status"], "RESPOSTA_REJEITADA")

    def test_orcamento(self):
        out, calls, _, _ = narrate.narrar([DIAG, DIAG], llm_fixo("Os 49 pedidos."), 1)
        self.assertEqual([o["llm_status"] for o in out], ["OK", "ORCAMENTO_ESGOTADO"])
        self.assertEqual(calls, 1)


if __name__ == "__main__":
    unittest.main()
