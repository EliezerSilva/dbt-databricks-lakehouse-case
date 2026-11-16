import unittest

from agent import investigate
from agent.db import GuardError, check_read, check_write

ORFAO = {"incidente_id": "i1", "fonte": "tabelas_orfas", "source_table": "silver_pedidos_itens",
         "classificacao": "AUSENTE_NA_ORIGEM", "ocorrencias": 2, "registros": 2, "pedidos": 2,
         "impacto_valor": 10.0, "resumo": "r", "proposta": "p",
         "evidencia": {"chaves": ["O00021", "O00099", "O00500"]}}
REGIAO = {**ORFAO, "incidente_id": "i2", "fonte": "tabelas_invalidas", "source_table": "silver_regioes",
          "evidencia": {"motivo": "m", "payload": {"regional_code": "XX"}}}
OUTRA_INVALIDA = {**REGIAO, "incidente_id": "i3", "source_table": "silver_clientes"}
DBT = {**ORFAO, "incidente_id": "i4", "fonte": "dbt", "evidencia": {}}


def linha(neg, chave=None, status=None, regra="item_sem_cabecalho", atual=True):
    return {"chave_negocio": neg, "regra": regra if chave else None, "chave": chave, "status": status,
            "ator": "sincronizacao" if chave else None, "ultimo_evento": "2026-11-16 22:00:00" if chave else None,
            "na_quarentena_atual": atual if chave else None}


class FakeDb:
    def __init__(self, rows):
        self.rows, self.consultas = rows, []

    def read(self, sql, params=None):
        check_read(sql)
        self.consultas.append((sql, params))
        return self.rows


def ferramentas(rows, amb="original"):
    db = FakeDb(rows)
    return investigate.Ferramentas(db, amb, [ORFAO, REGIAO, OUTRA_INVALIDA, DBT]), db


class TestEstadoQuarentena(unittest.TestCase):
    def test_orfaos_agrupados_por_regra_e_status(self):
        rows = [linha("O00021", "O00021-1", "REPROCESSADO", atual=False),
                linha("O00021", "O00021-2", "REPROCESSADO", atual=False),
                linha("O00099", "O00099-1", "QUARENTENADO"), linha("O00500")]
        f, db = ferramentas(rows)
        out = f.executar("estado_quarentena", {"incidente_id": "i1"})
        self.assertEqual(out["entidade"], "pedidos_itens")
        self.assertEqual(out["registros"], [
            {"regra": "item_sem_cabecalho", "status": "QUARENTENADO", "registros": 1},
            {"regra": "item_sem_cabecalho", "status": "REPROCESSADO", "registros": 2}])
        self.assertEqual((out["chaves_fora_da_quarentena"], out["exemplos_fora"]), (1, ["O00500"]))
        self.assertEqual(len(out["exemplos"]), 3)
        sql, params = db.consultas[0]
        self.assertIn("workspace.dbt_quarentena.quarentena_eventos", sql)
        self.assertEqual(params["entidade"], "pedidos_itens")
        self.assertEqual(investigate._linhas(out), 2)

    def test_regiao_invalida_usa_codigo_do_payload(self):
        f, db = ferramentas([linha("XX", "XX", "REJEITADO", regra="active_flag=0 ou regional_name nulo")])
        out = f.executar("estado_quarentena", {"incidente_id": "i2"})
        self.assertEqual(out["entidade"], "regioes")
        self.assertEqual(out["registros"][0]["status"], "REJEITADO")
        self.assertEqual(db.consultas[0][1]["chaves"], '["XX"]')

    def test_incidente_sem_quarentena_correspondente(self):
        for iid in ("i3", "i4"):
            f, db = ferramentas([])
            with self.assertRaises(ValueError):
                f.executar("estado_quarentena", {"incidente_id": iid})
            self.assertEqual(db.consultas, [])

    def test_indisponivel_fora_do_original(self):
        for amb in ("dev", "cenarios"):
            f, db = ferramentas([], amb)
            with self.assertRaises(ValueError):
                f.executar("estado_quarentena", {"incidente_id": "i1"})
            self.assertEqual(db.consultas, [])

    def test_argumentos_validados(self):
        investigate.validar_args("estado_quarentena", {"incidente_id": "i1"}, {"i1"})
        with self.assertRaises(ValueError):
            investigate.validar_args("estado_quarentena", {"incidente_id": "i9"}, {"i1"})
        with self.assertRaises(ValueError):
            investigate.validar_args("estado_quarentena", {"incidente_id": "i1", "entidade": "x"}, {"i1"})

    def test_somente_leitura(self):
        sql = investigate.ESTADO_QUARENTENA.format(q=investigate.QUARENTENA["original"])
        check_read(sql)
        with self.assertRaises(GuardError):
            check_write(sql)
        for proibido in ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP"):
            self.assertNotIn(proibido, sql.upper().split())

    def test_ferramenta_declarada_para_o_llm(self):
        nomes = [t["function"]["name"] for t in investigate.TOOLS]
        self.assertIn("estado_quarentena", nomes)
        self.assertEqual(set(nomes), set(investigate._PARAMS))


class TestQuarentenaNoDetalhe(unittest.TestCase):
    def test_estado_embutido_so_onde_atende(self):
        f, db = ferramentas([linha("O00099", "O00099-1", "QUARENTENADO")])
        out = f.executar("detalhar_incidente", {"incidente_id": "i1"})
        self.assertEqual(out["quarentena"]["registros"],
                         [{"regra": "item_sem_cabecalho", "status": "QUARENTENADO", "registros": 1}])
        self.assertEqual(len(db.consultas), 1)
        for iid in ("i3", "i4"):
            f, db = ferramentas([])
            self.assertNotIn("quarentena", f.executar("detalhar_incidente", {"incidente_id": iid}))
            self.assertEqual(db.consultas, [])

    def test_sem_quarentena_fora_do_original(self):
        for amb in ("dev", "cenarios"):
            f, db = ferramentas([], amb)
            self.assertNotIn("quarentena", f.executar("detalhar_incidente", {"incidente_id": "i1"}))
            self.assertEqual(db.consultas, [])

    def test_falha_de_leitura_nao_derruba_o_detalhe(self):
        f, _ = ferramentas([])
        f.db.read = lambda sql, params=None: (_ for _ in ()).throw(RuntimeError("x"))
        out = f.executar("detalhar_incidente", {"incidente_id": "i1"})
        self.assertEqual(out["quarentena"], {"erro": "falha ao ler a quarentena"})


if __name__ == "__main__":
    unittest.main()
