import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

READ_PREFIXES = ("SELECT ", "WITH ")
WRITE_PREFIXES = ("INSERT INTO WORKSPACE.AGENT.", "CREATE TABLE IF NOT EXISTS WORKSPACE.AGENT.")


class GuardError(RuntimeError):
    pass


def _norm(sql):
    return " ".join(sql.split()).upper().rstrip(";")


def check_read(sql):
    s = _norm(sql)
    if not s.startswith(READ_PREFIXES) or ";" in s:
        raise GuardError("leitura aceita apenas um SELECT/WITH")


def check_write(sql):
    s = _norm(sql)
    if not s.startswith(WRITE_PREFIXES) or ";" in s:
        raise GuardError("escrita permitida apenas em workspace.agent (INSERT/CREATE TABLE IF NOT EXISTS)")


def _param(name, v):
    if isinstance(v, tuple):
        value, typ = v
    elif isinstance(v, bool):
        value, typ = v, "BOOLEAN"
    elif isinstance(v, int):
        value, typ = v, "BIGINT"
    elif isinstance(v, float):
        value, typ = v, "DOUBLE"
    else:
        value, typ = v, "STRING"
    if isinstance(value, bool):
        value = str(value).lower()
    return StatementParameterListItem(name=name, value=None if value is None else str(value), type=typ)


def _conv(v, typ):
    if v is None:
        return None
    t = (getattr(typ, "value", typ) or "").upper()
    if t in ("INT", "LONG", "SHORT", "BYTE"):
        return int(v)
    if t in ("DOUBLE", "FLOAT", "DECIMAL"):
        return float(v)
    if t == "BOOLEAN":
        return v.lower() == "true"
    return v


class Db:
    def __init__(self, profile, warehouse_id, timeout_s=300):
        self.w = WorkspaceClient(profile=profile)
        self.warehouse_id = warehouse_id
        self.timeout_s = timeout_s
        self.calls = 0

    def _exec(self, sql, params):
        p = [_param(k, v) for k, v in (params or {}).items()] or None
        r = self.w.statement_execution.execute_statement(
            statement=sql, warehouse_id=self.warehouse_id, wait_timeout="50s", parameters=p)
        limite = time.monotonic() + self.timeout_s
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.monotonic() > limite:
                self.w.statement_execution.cancel_execution(r.statement_id)
                raise RuntimeError("timeout de consulta")
            time.sleep(2)
            r = self.w.statement_execution.get_statement(r.statement_id)
        self.calls += 1
        if r.status.state != StatementState.SUCCEEDED:
            msg = r.status.error.message if r.status.error else r.status.state.value
            raise RuntimeError(f"SQL falhou: {msg}")
        if not (r.manifest and r.manifest.schema and r.manifest.schema.columns):
            return []
        if (r.manifest.total_chunk_count or 1) > 1:
            raise RuntimeError("resultado excede um chunk; restrinja a consulta")
        cols = r.manifest.schema.columns
        rows = (r.result.data_array if r.result else None) or []
        return [{c.name: _conv(v, c.type_name) for c, v in zip(cols, row)} for row in rows]

    def read(self, sql, params=None):
        check_read(sql)
        return self._exec(sql, params)

    def write(self, sql, params=None):
        check_write(sql)
        return self._exec(sql, params)
