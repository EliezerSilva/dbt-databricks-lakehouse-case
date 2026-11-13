import unittest

from agent import estado, investigate
from agent.db import check_read, check_write


def diag(iid, fonte="tabelas_orfas"):
    return {"incidente_id": iid, "fonte": fonte, "classificacao": "AUSENTE_NA_ORIGEM",
            "source_table": "fato_pedidos", "resumo": "Resumo."}


def st(status, fonte="tabelas_orfas", nota=None, deteccoes=1, reaberturas=0):
    return {"status": status, "fonte": fonte, "classificacao": "AUSENTE_NA_ORIGEM",
            "source_table": "fato_pedidos", "nota": nota, "deteccoes": deteccoes, "reaberturas": reaberturas}


TODAS = {"tabelas_invalidas", "tabelas_orfas", "ingestion_log", "dbt"}


def evs(estado_atual, diags, fontes=TODAS):
    return {e["incidente_id"]: (e["evento"], e["status_resultante"]) for e in estado.transicoes(estado_atual, diags, fontes)}


class TestTransicoes(unittest.TestCase):
    def test_novo(self):
        self.assertEqual(evs({}, [diag("a")]), {"a": ("NOVO", "ABERTO")})

    def test_detectado_aberto(self):
        self.assertEqual(evs({"a": st("ABERTO")}, [diag("a")]), {"a": ("DETECTADO", "ABERTO")})

    def test_reconhecido_permanece_e_carrega_nota(self):
        out = estado.transicoes({"a": st("RECONHECIDO", nota="ERP acionado")}, [diag("a")], TODAS)
        self.assertEqual((out[0]["evento"], out[0]["status_resultante"], out[0]["nota"]), ("DETECTADO", "RECONHECIDO", "ERP acionado"))

    def test_reincidencia(self):
        self.assertEqual(evs({"a": st("RESOLVIDO")}, [diag("a")]), {"a": ("REABERTO", "ABERTO")})

    def test_resolucao(self):
        self.assertEqual(evs({"a": st("ABERTO"), "b": st("RECONHECIDO")}, []),
                         {"a": ("RESOLVIDO", "RESOLVIDO"), "b": ("RESOLVIDO", "RESOLVIDO")})

    def test_resolvido_ausente_sem_evento(self):
        self.assertEqual(evs({"a": st("RESOLVIDO")}, []), {})

    def test_nao_resolve_se_a_fonte_nao_foi_avaliada(self):
        self.assertEqual(evs({"a": st("ABERTO", fonte="dbt")}, [], fontes={"tabelas_orfas"}), {})


class TestAnotacao(unittest.TestCase):
    def test_reincidente_e_alerta(self):
        d = diag("a")
        eventos = estado.transicoes({"a": st("RESOLVIDO", deteccoes=2)}, [d], TODAS)
        estado.anotar([d], {"a": st("RESOLVIDO", deteccoes=2)}, eventos)
        self.assertTrue(d["estado"]["reincidente"])
        self.assertEqual(d["estado"]["reaberturas"], 1)
        d.update({"proposta": "p"})
        self.assertEqual(investigate.alerta(d)["historico"]["reaberturas"], 1)

    def test_suprimida_com_nota(self):
        d = diag("a")
        estado.anotar([d], {"a": st("RECONHECIDO", nota="ERP acionado")},
                      estado.transicoes({"a": st("RECONHECIDO", nota="ERP acionado")}, [d], TODAS))
        n = estado.narrativa_suprimida(d)
        self.assertEqual(n["llm_status"], "SUPRIMIDO_RECONHECIDO")
        self.assertIn("ERP acionado", n["explicacao"])

    def test_sem_evento_nao_quebra(self):
        d = diag("a")
        estado.anotar([d], {}, [])
        self.assertEqual(d["estado"]["evento"], "SEM_ESTADO")


class TestReincidencia(unittest.TestCase):
    def test_prefixo_quando_narrativa_nao_menciona(self):
        d = diag("a")
        d["estado"] = {"reincidente": True, "reaberturas": 1}
        narrs = [{"incidente_id": "a", "explicacao": "Layout divergente."}]
        estado.marcar_reincidencia(narrs, [d])
        self.assertTrue(narrs[0]["explicacao"].startswith("Incidente reincidente (reaberto 1 vez(es))."))

    def test_sem_duplicar_quando_ja_menciona(self):
        d = diag("a")
        d["estado"] = {"reincidente": True, "reaberturas": 1}
        narrs = [{"incidente_id": "a", "explicacao": "Incidente reincidente no layout."}]
        estado.marcar_reincidencia(narrs, [d])
        self.assertEqual(narrs[0]["explicacao"], "Incidente reincidente no layout.")


class TestSqlEstado(unittest.TestCase):
    def test_guardas(self):
        check_write(estado.DDL)
        check_write(estado.INSERT)
        check_read(estado.ESTADO)


if __name__ == "__main__":
    unittest.main()
