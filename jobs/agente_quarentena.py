import sys

raiz = sys.argv[1]
sys.path.insert(0, raiz)

from agent import run  # noqa: E402

rc = run.main(sys.argv[2:])
if rc:
    raise SystemExit(rc)
