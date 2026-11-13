import unittest

from agent import classify, investigate, run, tools
from agent.db import check_read


def linha(dim, valor, receita, pedidos=1):
    return {"dimensao": dim, "valor": valor, "receita": str(receita), "pedidos": str(pedidos)}


class FakeDb:
    def __init__(self, rows):
        self.rows, self.consultas = rows, []

    def read(self, sql, params=None):
        self.consultas.append(sql)
        return self.rows


ROWS = [
    linha("canal", "Inside Sales", 844.0), linha("canal", "Sem canal", 156.0, 39),
    linha("regiao", "Sul", 995.0), linha("regiao", "Sem região", 5.0, 2),
    linha("segmento", "Varejo", 1000.0),
]


class TestCobertura(unittest.TestCase):
    def test_lacuna_acima_do_limiar_vira_incidente(self):
        diags = run.diagnosticos_cobertura(FakeDb(ROWS), "original")
        self.assertEqual([d["evidencia"]["dimensao"] for d in diags], ["canal"])
        d = diags[0]
        self.assertEqual(d["classificacao"], classify.LACUNA_DE_ATRIBUICAO)
        self.assertEqual((d["impacto_valor"], d["pedidos"], d["evidencia"]["parcela_pct"]), (156.0, 39, 15.6))
        self.assertTrue(d["decisao_humana"])

    def test_limiar_configuravel(self):
        diags = run.diagnosticos_cobertura(FakeDb(ROWS), "original", limiar=0.2)
        self.assertEqual(diags, [])

    def test_ambiente_sem_camada_semantica(self):
        db = FakeDb(ROWS)
        self.assertEqual(run.diagnosticos_cobertura(db, "cenarios"), [])
        self.assertEqual(db.consultas, [])

    def test_id_estavel_por_dimensao(self):
        a = run.diagnosticos_cobertura(FakeDb(ROWS), "original")[0]["incidente_id"]
        b = run.diagnosticos_cobertura(FakeDb(ROWS), "original")[0]["incidente_id"]
        self.assertEqual(a, b)


class TestFerramentaImpacto(unittest.TestCase):
    def test_dimensao_validada(self):
        investigate.validar_args("impacto_metricas", {"dimensao": "canal"}, set())
        for ruim in ("canal; DROP TABLE x", "nao_existe", 1):
            with self.assertRaises(ValueError):
                investigate.validar_args("impacto_metricas", {"dimensao": ruim}, set())

    def test_indisponivel_fora_do_original(self):
        f = investigate.Ferramentas(FakeDb([]), "cenarios", [])
        with self.assertRaises(ValueError):
            f.executar("impacto_metricas", {"dimensao": "canal"})

    def test_distribuicao_no_original(self):
        db = FakeDb([{"valor": "Sem canal", "receita_liquida": "527929.23", "pedidos": "39"}])
        out = investigate.Ferramentas(db, "original", []).executar("impacto_metricas", {"dimensao": "canal"})
        self.assertEqual(investigate._linhas(out), 1)
        self.assertIn("GROUP BY canal", db.consultas[0])


class TestSqlSemantica(unittest.TestCase):
    def test_guardas(self):
        check_read(tools.COBERTURA.format(mv=tools.SEMANTICA["original"]))
        for dim in investigate.DIMENSOES:
            check_read(investigate.IMPACTO.format(dim=dim, mv=tools.SEMANTICA["original"]))


if __name__ == "__main__":
    unittest.main()
