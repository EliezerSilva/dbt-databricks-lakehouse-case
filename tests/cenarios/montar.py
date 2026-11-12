import re
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

PERMITIDO = re.compile(
    r"^(CREATE SCHEMA IF NOT EXISTS WORKSPACE\.[A-Z]+_CEN\b"
    r"|CREATE OR REPLACE TABLE WORKSPACE\.[A-Z]+_CEN\.\w+"
    r"|INSERT INTO WORKSPACE\.[A-Z]+_CEN\.\w+)")


def statements(texto):
    texto = "\n".join(l for l in texto.splitlines() if not l.strip().startswith("--"))
    partes, atual, em_aspas = [], [], False
    for ch in texto:
        if ch == "'":
            em_aspas = not em_aspas
        if ch == ";" and not em_aspas:
            partes.append("".join(atual))
            atual = []
        else:
            atual.append(ch)
    partes.append("".join(atual))
    return [" ".join(s.split()) for s in partes if s.strip()]


def main():
    stmts = statements(Path(__file__).with_name("montar_cenarios.sql").read_text())
    bloqueados = [s for s in stmts if not PERMITIDO.match(s.upper())]
    if bloqueados:
        sys.exit(f"bloqueado (nada executado): {bloqueados[0][:100]}")
    if "--validar" in sys.argv:
        for i, s in enumerate(stmts, 1):
            print(i, s[:110])
        return
    w = WorkspaceClient(profile="dbt-lakehouse-free")
    for s in stmts:
        r = w.statement_execution.execute_statement(statement=s, warehouse_id="6ad31e4709e166fb", wait_timeout="50s")
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(2)
            r = w.statement_execution.get_statement(r.statement_id)
        print(r.status.state.value, s[:90])
        if r.status.state != StatementState.SUCCEEDED:
            sys.exit(r.status.error.message if r.status.error else "falha")


if __name__ == "__main__":
    main()
