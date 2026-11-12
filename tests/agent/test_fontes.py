import unittest

from agent import classify, dbt_artifacts, dominios


class TestDominios(unittest.TestCase):
    def test_normalizacao(self):
        self.assertEqual(dominios.normalizar("Sul."), "SUL")
        self.assertEqual(dominios.normalizar("Região Sul"), "REGIAOSUL")

    def test_correspondencia(self):
        self.assertEqual(dominios.correspondencia("dim_regiao", "regional_code", "Sul."), "S")
        self.assertEqual(dominios.correspondencia("dim_regiao", "regional_code", "s"), "S")
        self.assertIsNone(dominios.correspondencia("dim_regiao", "regional_code", "XX"))
        self.assertIsNone(dominios.correspondencia("dim_x", "y", "S"))


class TestClassificacaoNovas(unittest.TestCase):
    def test_dominio_com_sinonimo(self):
        cls, resumo, humano, proposta = classify.classify_dominio("dim_regiao", "regional_code", [("SUL.", 1)])
        self.assertEqual(cls, classify.DRIFT_DE_DOMINIO)
        self.assertIn("'S'", proposta)
        self.assertTrue(humano)

    def test_dominio_sem_valores(self):
        self.assertEqual(classify.classify_dominio("dim_regiao", "regional_code", [])[0], classify.FALHA_TESTE_DBT)

    def test_layout(self):
        self.assertEqual(classify.classify_layout("sep=; cols=5", "sep=; cols=6", "x")[0], classify.DRIFT_DE_LAYOUT)
        self.assertEqual(classify.classify_layout("sep=; cols=6", "sep=; cols=6", "x")[0], classify.FALHA_DE_CARGA)


class TestDbtArtifacts(unittest.TestCase):
    RR = {"args": {"target": "cenarios"}, "metadata": {"generated_at": "t"}, "results": [
        {"unique_id": "test.p.av", "status": "fail", "failures": 1, "message": "Got 1 result"},
        {"unique_id": "test.p.w", "status": "warn", "failures": 1, "message": "Got 1 result"},
        {"unique_id": "test.p.e", "status": "error", "failures": None, "message": "TABLE_OR_VIEW_NOT_FOUND"}]}
    MF = {"nodes": {
        "test.p.av": {"test_metadata": {"name": "accepted_values",
                                         "kwargs": {"column_name": "regional_code", "values": ["S", "N"]}},
                      "attached_node": "model.p.dim_regiao"},
        "model.p.dim_regiao": {"name": "dim_regiao", "relation_name": "`workspace`.`dbt_cen_gold`.`dim_regiao`"}}}

    def test_falhas_ignora_warn(self):
        f = dbt_artifacts.falhas(self.RR, self.MF)
        self.assertEqual(len(f), 1)
        self.assertEqual((f[0]["teste"], f[0]["coluna"], f[0]["modelo"]), ("accepted_values", "regional_code", "dim_regiao"))
        self.assertEqual(f[0]["valores"], ["S", "N"])

    def test_erro_de_execucao_nao_e_falha_de_dado(self):
        self.assertEqual(dbt_artifacts.erros_execucao(self.RR), ["test.p.e"])
        self.assertNotIn("test.p.e", [f["unique_id"] for f in dbt_artifacts.falhas(self.RR, self.MF)])

    def test_target(self):
        self.assertEqual(dbt_artifacts.target_de(self.RR), "cenarios")
        self.assertEqual(dbt_artifacts.target_de({}), "dev")

    def test_identificadores(self):
        dbt_artifacts.validar_identificadores("`workspace`.`dbt_cen_gold`.`dim_regiao`", "regional_code")
        for rel, col in (("`workspace`.`silver`.`x`", "a"), ("`workspace`.`dbt_gold`.`x`", "a; drop")):
            with self.assertRaises(ValueError):
                dbt_artifacts.validar_identificadores(rel, col)


if __name__ == "__main__":
    unittest.main()
