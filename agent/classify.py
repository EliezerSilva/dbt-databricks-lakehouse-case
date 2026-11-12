DESCARTE_LEGITIMO = "DESCARTE_LEGITIMO"
DADO_INCOMPLETO_ORIGEM = "DADO_INCOMPLETO_ORIGEM"
AUSENTE_NA_ORIGEM = "AUSENTE_NA_ORIGEM"
FALHA_DE_CARGA = "FALHA_DE_CARGA"
DESCARTE_A_MONTANTE = "DESCARTE_A_MONTANTE"
ORFAO_DESATUALIZADO = "ORFAO_DESATUALIZADO"
NAO_CLASSIFICADO = "NAO_CLASSIFICADO"

TODAS = (DESCARTE_LEGITIMO, DADO_INCOMPLETO_ORIGEM, AUSENTE_NA_ORIGEM, FALHA_DE_CARGA,
         DESCARTE_A_MONTANTE, ORFAO_DESATUALIZADO, NAO_CLASSIFICADO)

PROPOSTAS = {
    DESCARTE_LEGITIMO: "Nenhuma acao. Descarte previsto pela regra de negocio.",
    DADO_INCOMPLETO_ORIGEM: "Solicitar correcao do cadastro na origem; nao liberar o registro.",
    AUSENTE_NA_ORIGEM: "Solicitar ao responsavel pela extracao do ERP os cabecalhos ausentes; manter itens em quarentena.",
    FALHA_DE_CARGA: "Reprocessar a carga Bronze do arquivo de cabecalho apos corrigir a falha registrada no ingestion_log.",
    DESCARTE_A_MONTANTE: "Investigar a regra da Silver que descartou o cabecalho; avaliar correcao antes de reprocessar.",
    ORFAO_DESATUALIZADO: "Cabecalho ja existe na Silver; reprocessar a Gold para limpar a quarentena.",
    NAO_CLASSIFICADO: "Sem regra de classificacao; requer analise humana.",
}


def _regioes(payload):
    if str(payload.get("active_flag")) == "0":
        return (DESCARTE_LEGITIMO,
                f"Regiao {payload.get('regional_code')} inativa na origem (active_flag=0).", False)
    if not payload.get("regional_name"):
        return DADO_INCOMPLETO_ORIGEM, "Regiao ativa sem regional_name.", True
    return NAO_CLASSIFICADO, "Registro de regiao em quarentena sem condicao reconhecida.", True


INVALID_RULES = {
    ("silver_regioes", "active_flag=0 ou regional_name nulo"): _regioes,
}


def classify_invalido(source_table, motivo, payload):
    rule = INVALID_RULES.get((source_table, motivo))
    if rule is None:
        return NAO_CLASSIFICADO, f"Motivo sem regra de classificacao: {motivo}", True
    return rule(payload)


def classify_orfao(na_bronze, na_silver, carga_ok):
    if na_silver:
        return ORFAO_DESATUALIZADO, "Cabecalho existe na Silver; quarentena anterior a carga do cabecalho.", True
    if na_bronze:
        return DESCARTE_A_MONTANTE, "Cabecalho chegou a Bronze e foi descartado na Silver.", True
    if carga_ok is False:
        return FALHA_DE_CARGA, "Cabecalho ausente da Bronze e a ultima carga do arquivo de cabecalho falhou.", True
    nota = "carga do arquivo de cabecalho OK" if carga_ok else "status da carga do cabecalho nao encontrado"
    return AUSENTE_NA_ORIGEM, f"Cabecalho nunca chegou a Bronze ({nota}).", True
