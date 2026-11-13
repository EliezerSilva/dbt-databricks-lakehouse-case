import re
import unicodedata

from agent import ontologia

DOMINIOS = ontologia.dominios()


def normalizar(valor):
    s = unicodedata.normalize("NFKD", str(valor)).encode("ascii", "ignore").decode().upper()
    return re.sub(r"[^A-Z0-9]", "", s)


def correspondencia(modelo, coluna, valor):
    dominio = DOMINIOS.get((modelo, coluna))
    if not dominio:
        return None
    n = normalizar(valor)
    for codigo, sinonimos in dominio.items():
        if n == normalizar(codigo) or n in {normalizar(x) for x in sinonimos}:
            return codigo
    return None
