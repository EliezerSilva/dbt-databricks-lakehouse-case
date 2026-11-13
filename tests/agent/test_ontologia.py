import copy
import unittest

from agent import classify, dominios, investigate, ontologia
from agent.db import check_read


class TestOntologia(unittest.TestCase):
    def test_consistencia_interna(self):
        self.assertEqual(ontologia.validar_interno(), [])

    def test_dominios_preservados(self):
        self.assertEqual(dominios.DOMINIOS, {("dim_regiao", "regional_code"): {
            "N": ["NORTE"], "NE": ["NORDESTE"], "SE": ["SUDESTE"], "S": ["SUL"], "CO": ["CENTRO-OESTE"]}})

    def test_cadeias(self):
        self.assertEqual(ontologia.cadeia("canal"),
                         "item_pedido -> pedido (via order_id) -> vendedor (via seller_id) -> canal (via canal_id)")
        self.assertTrue(ontologia.cadeia("segmento").endswith("cliente (via customer_code), atributo segmento"))

    def test_sql_causa(self):
        for d in ontologia.atribuicoes():
            check_read(ontologia.sql_causa(d))
        self.assertIn("LEFT JOIN workspace.dbt_gold.dim_canal y ON x.canal_id = y.canal_id", ontologia.sql_causa("canal"))
        self.assertIn("WHERE segmento IS NULL", ontologia.sql_causa("segmento"))

    def test_genero_no_resumo(self):
        self.assertIn("sem região atribuída", classify.classify_lacuna("regiao", 4.5)[1])
        self.assertIn("sem canal atribuído", classify.classify_lacuna("canal", 15.6)[1])

    def test_dimensoes_da_ferramenta_existem(self):
        dims = {d for e in ontologia.carregar()["entidades"].values() for d in e.get("dimensoes_semanticas", [])}
        self.assertTrue(set(investigate.DIMENSOES) <= dims)

    def test_inconsistencia_detectada(self):
        o = copy.deepcopy(ontologia.carregar())
        o["atribuicao"]["canal"]["caminho"] = ["item_pedido", "canal"]
        o["entidades"]["x"] = {"modelo": "dim x", "chave": "id"}
        erros = ontologia.validar_interno(o)
        self.assertTrue(any("relacao ausente item_pedido -> canal" in e for e in erros))
        self.assertTrue(any("identificador invalido 'dim x'" in e for e in erros))

    def test_manifest_desatualizado(self):
        manifest = {"nodes": {"model.p.dim_canal": {"resource_type": "model", "name": "dim_canal", "raw_code": ""}}}
        erros = ontologia.validar_manifest(manifest)
        self.assertTrue(any("dim_cliente ausente no manifest" in e for e in erros))
        self.assertTrue(any("metrica receita_liquida ausente" in e for e in erros))

    def test_amostra_de_lacuna_usa_a_ontologia(self):
        class FakeDb:
            def __init__(self):
                self.consultas = []

            def read(self, sql, params=None):
                self.consultas.append(sql)
                return [{"chave": f"V00{i}", "valor": None} for i in range(7)]
        d = {"incidente_id": "lac", "fonte": "cobertura_semantica", "evidencia": {"dimensao": "canal"}}
        db = FakeDb()
        out = investigate.Ferramentas(db, "original", [d]).executar("amostrar_registros", {"incidente_id": "lac", "limite": 5})
        self.assertIn("LEFT JOIN workspace.dbt_gold.dim_canal", db.consultas[0])
        self.assertEqual((len(out["registros"]), out["registros_sem_correspondencia"]), (5, 7))
        self.assertTrue(out["cadeia"].endswith("canal (via canal_id)"))
        with self.assertRaises(ValueError):
            investigate.Ferramentas(FakeDb(), "cenarios", [d]).executar("amostrar_registros", {"incidente_id": "lac", "limite": 5})


if __name__ == "__main__":
    unittest.main()
