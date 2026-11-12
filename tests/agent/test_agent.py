import unittest

from agent import classify
from agent.db import GuardError, check_read, check_write

MOTIVO_REGIOES = "active_flag=0 ou regional_name nulo"


class TestClassificacao(unittest.TestCase):
    def test_regiao_inativa_e_descarte_legitimo(self):
        cls, _, humano = classify.classify_invalido(
            "silver_regioes", MOTIVO_REGIOES, {"regional_code": "XX", "active_flag": "0"})
        self.assertEqual(cls, classify.DESCARTE_LEGITIMO)
        self.assertFalse(humano)

    def test_regiao_ativa_sem_nome(self):
        cls, _, humano = classify.classify_invalido(
            "silver_regioes", MOTIVO_REGIOES, {"regional_code": "N", "active_flag": "1"})
        self.assertEqual(cls, classify.DADO_INCOMPLETO_ORIGEM)
        self.assertTrue(humano)

    def test_motivo_desconhecido(self):
        cls, _, humano = classify.classify_invalido("silver_x", "motivo novo", {})
        self.assertEqual(cls, classify.NAO_CLASSIFICADO)
        self.assertTrue(humano)

    def test_orfaos(self):
        self.assertEqual(classify.classify_orfao(False, False, True)[0], classify.AUSENTE_NA_ORIGEM)
        self.assertEqual(classify.classify_orfao(False, False, None)[0], classify.AUSENTE_NA_ORIGEM)
        self.assertEqual(classify.classify_orfao(False, False, False)[0], classify.FALHA_DE_CARGA)
        self.assertEqual(classify.classify_orfao(True, False, True)[0], classify.DESCARTE_A_MONTANTE)
        self.assertEqual(classify.classify_orfao(True, True, True)[0], classify.ORFAO_DESATUALIZADO)

    def test_toda_classificacao_tem_proposta(self):
        for c in classify.TODAS:
            self.assertIn(c, classify.PROPOSTAS)


class TestGuardas(unittest.TestCase):
    def test_leitura_permitida(self):
        check_read("SELECT 1")
        check_read("  with x as (select 1) select * from x;")

    def test_leitura_bloqueada(self):
        for sql in ("DELETE FROM workspace.silver.tabelas_invalidas",
                    "SELECT 1; DROP TABLE workspace.silver.quality_log",
                    "INSERT INTO workspace.agent.diagnosticos VALUES (1)",
                    "MERGE INTO workspace.silver.tabelas_orfas t USING x ON 1=1"):
            with self.assertRaises(GuardError, msg=sql):
                check_read(sql)

    def test_escrita_permitida(self):
        check_write("INSERT INTO workspace.agent.diagnosticos VALUES (1)")
        check_write("CREATE TABLE IF NOT EXISTS workspace.agent.execucoes (a INT)")

    def test_escrita_bloqueada(self):
        for sql in ("INSERT INTO workspace.silver.quality_log VALUES (1)",
                    "DELETE FROM workspace.agent.diagnosticos",
                    "CREATE TABLE workspace.gold.x (a INT)",
                    "INSERT INTO workspace.agent.x VALUES (1); DROP TABLE workspace.gold.dim_canal"):
            with self.assertRaises(GuardError, msg=sql):
                check_write(sql)


if __name__ == "__main__":
    unittest.main()


class TestSqlDoAgente(unittest.TestCase):
    def test_sql_reais_passam_nas_guardas(self):
        from agent import run, tools
        for ddl in run.DDL:
            check_write(ddl)
        for ins in (run.INSERT_DIAG, run.INSERT_EXEC, run.INSERT_NARR):
            check_write(ins)
        for amb in tools.AMBIENTES:
            for t in tools.TEMPLATES_AMBIENTE:
                check_read(tools.sql(t, amb))
        check_read(tools.VALORES_FORA.format(rel="`workspace`.`dbt_gold`.`dim_regiao`", col="regional_code"))

    def test_ambiente_fora_da_lista(self):
        from agent import tools
        with self.assertRaises(KeyError):
            tools.sql(tools.INVALIDOS, "prod")
