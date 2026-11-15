import io
import json
import sys
from contextlib import redirect_stdout

raiz = sys.argv[1]
sys.path.insert(0, raiz)

from agent import run  # noqa: E402

buf = io.StringIO()
with redirect_stdout(buf):
    rc = run.main(sys.argv[2:])
saida = buf.getvalue()
print(saida)
status = json.loads(saida).get("status") if saida.strip() else None
if rc or status != "OK":
    raise SystemExit(f"agente terminou com status {status}")
