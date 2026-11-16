import unittest

from agent.db import GuardError, check_read, check_write
from quarentena import ciclo

K = ("pedidos_itens", "item_sem_cabecalho", "O00021-1")
K2 = ("pedidos_itens", "item_sem_cabecalho", "O00021-2")
R = ("regioes", "active_flag=0 ou regional_name nulo", "XX")


def st(status, origem="consumidor_dbt"):
    return {"status": status, "origem": origem}


def evs(estado, atuais, alvo=frozenset()):
    return [(e["chave"], e["evento"], e["status_resultante"]) for e in ciclo.transicoes(estado, atuais, set(alvo))]


class TestTransicoes(unittest.TestCase):
    def test_primeira_sincronizacao(self):
        r = evs({}, {K: "consumidor_dbt", R: "produtor_pyspark"})
        self.assertEqual(sorted(r), sorted([("O00021-1", "QUARENTENADO", "QUARENTENADO"), ("XX", "QUARENTENADO", "QUARENTENADO")]))

    def test_idempotente(self):
        self.assertEqual(evs({K: st("QUARENTENADO")}, {K: "consumidor_dbt"}), [])

    def test_saiu_da_quarentena_sem_destino(self):
        self.assertEqual(evs({K: st("QUARENTENADO")}, {}), [("O00021-1", "ELEGIVEL", "ELEGIVEL")])

    def test_saiu_e_ja_esta_no_destino(self):
        r = evs({K: st("QUARENTENADO")}, {}, {("pedidos_itens", "O00021-1")})
        self.assertEqual(r, [("O00021-1", "ELEGIVEL", "ELEGIVEL"), ("O00021-1", "REPROCESSADO", "REPROCESSADO")])

    def test_elegivel_chega_no_destino(self):
        r = evs({K: st("ELEGIVEL")}, {}, {("pedidos_itens", "O00021-1")})
        self.assertEqual(r, [("O00021-1", "REPROCESSADO", "REPROCESSADO")])

    def test_reprocessado_e_estavel(self):
        self.assertEqual(evs({K: st("REPROCESSADO")}, {}, {("pedidos_itens", "O00021-1")}), [])

    def test_reincidencia(self):
        self.assertEqual(evs({K: st("REPROCESSADO")}, {K: "consumidor_dbt"}), [("O00021-1", "REINCIDENTE", "QUARENTENADO")])

    def test_terminais_nao_mudam(self):
        estado = {K: st("REJEITADO"), K2: st("EXCECAO_APROVADA")}
        self.assertEqual(evs(estado, {K: "consumidor_dbt"}), [])
        self.assertEqual(evs(estado, {}, {("pedidos_itens", "O00021-1")}), [])


class TestAcaoHumana(unittest.TestCase):
    def test_rejeitar(self):
        ev = ciclo.acao_humana({K: st("QUARENTENADO")}, K, "rejeitar", "eli", "pedido cancelado na origem")
        self.assertEqual((ev["evento"], ev["ator"]), ("REJEITADO", "eli"))

    def test_excecao_so_de_quarentenado(self):
        with self.assertRaises(ValueError):
            ciclo.acao_humana({K: st("ELEGIVEL")}, K, "aprovar-excecao", "eli", "nota")

    def test_exige_ator_e_nota(self):
        with self.assertRaises(ValueError):
            ciclo.acao_humana({K: st("QUARENTENADO")}, K, "rejeitar", "eli", "")

    def test_registro_inexistente(self):
        with self.assertRaises(ValueError):
            ciclo.acao_humana({}, K, "rejeitar", "eli", "nota")

    def test_terminal_nao_volta(self):
        with self.assertRaises(ValueError):
            ciclo.acao_humana({K: st("REJEITADO")}, K, "rejeitar", "eli", "nota")


class TestGuardas(unittest.TestCase):
    def test_sql_do_ciclo_passa(self):
        check_write(ciclo.DDL)
        check_write("INSERT INTO workspace.dbt_quarentena.quarentena_eventos VALUES (:seq_0)")
        for sql in (ciclo.ESTADO, ciclo.ATUAIS, ciclo.ALVO, ciclo.RESUMO):
            check_read(sql)

    def test_escrita_restrita_a_tabela_de_eventos(self):
        for sql in ("INSERT INTO workspace.dbt_quarentena.quarentena_registros VALUES (1)",
                    "INSERT INTO workspace.dbt_quarentena.quarentena_eventos_x VALUES (1)",
                    "DELETE FROM workspace.dbt_quarentena.quarentena_eventos"):
            with self.assertRaises(GuardError, msg=sql):
                check_write(sql)


class TestGravacao(unittest.TestCase):
    def test_lotes(self):
        class FakeDb:
            def __init__(self):
                self.sqls = []

            def write(self, sql, params=None):
                self.sqls.append((sql, params))

        db = FakeDb()
        eventos = [ciclo._ev(("pedidos_itens", "item_sem_cabecalho", f"O{i}-1"), "consumidor_dbt", "QUARENTENADO", "QUARENTENADO") for i in range(23)]
        self.assertEqual(ciclo.gravar(db, eventos), 23)
        inserts = [s for s in db.sqls if s[0].startswith("INSERT")]
        self.assertEqual(len(inserts), 3)
        self.assertEqual(sum(len(p) for _, p in inserts), 23 * len(ciclo.COLUNAS))
        for sql, _ in inserts:
            check_write(sql)


if __name__ == "__main__":
    unittest.main()
