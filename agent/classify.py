from agent import dominios

DESCARTE_LEGITIMO = "DESCARTE_LEGITIMO"
DADO_INCOMPLETO_ORIGEM = "DADO_INCOMPLETO_ORIGEM"
AUSENTE_NA_ORIGEM = "AUSENTE_NA_ORIGEM"
FALHA_DE_CARGA = "FALHA_DE_CARGA"
DESCARTE_A_MONTANTE = "DESCARTE_A_MONTANTE"
ORFAO_DESATUALIZADO = "ORFAO_DESATUALIZADO"
DRIFT_DE_LAYOUT = "DRIFT_DE_LAYOUT"
DRIFT_DE_DOMINIO = "DRIFT_DE_DOMINIO"
FALHA_TESTE_DBT = "FALHA_TESTE_DBT"
NAO_CLASSIFICADO = "NAO_CLASSIFICADO"

TODAS = (DESCARTE_LEGITIMO, DADO_INCOMPLETO_ORIGEM, AUSENTE_NA_ORIGEM, FALHA_DE_CARGA, DESCARTE_A_MONTANTE,
         ORFAO_DESATUALIZADO, DRIFT_DE_LAYOUT, DRIFT_DE_DOMINIO, FALHA_TESTE_DBT, NAO_CLASSIFICADO)

PROPOSTAS = {
    DESCARTE_LEGITIMO: "Nenhuma acao. Descarte previsto pela regra de negocio.",
    DADO_INCOMPLETO_ORIGEM: "Solicitar correcao do cadastro na origem; nao liberar o registro.",
    AUSENTE_NA_ORIGEM: "Solicitar ao responsavel pela extracao do ERP os cabecalhos ausentes; manter itens em quarentena.",
    FALHA_DE_CARGA: "Corrigir a falha registrada no ingestion_log e reprocessar a carga Bronze do arquivo.",
    DESCARTE_A_MONTANTE: "Investigar a regra da Silver que descartou o cabecalho; avaliar correcao antes de reprocessar.",
    ORFAO_DESATUALIZADO: "Cabecalho ja existe na Silver; reprocessar a Gold para limpar a quarentena.",
    DRIFT_DE_LAYOUT: "Confirmar com o dono da fonte se a mudanca de layout e intencional; se for, versionar o novo layout e ajustar a leitura via PR. Carga segue bloqueada.",
    DRIFT_DE_DOMINIO: "Avaliar inclusao dos valores no dominio ou como sinonimos, via PR; nao liberar dados automaticamente.",
    FALHA_TESTE_DBT: "Analisar o teste dbt com falha; requer decisao humana.",
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


def classify_layout(detectado, esperado, mensagem):
    if detectado and esperado and detectado != esperado:
        return DRIFT_DE_LAYOUT, f"Layout recebido ({detectado}) difere do esperado ({esperado}); carga bloqueada.", True
    return FALHA_DE_CARGA, f"Carga em NOK sem divergencia de layout: {mensagem}", True


def classify_dominio(modelo, coluna, valores):
    if not valores:
        return (FALHA_TESTE_DBT, f"accepted_values de {modelo}.{coluna} falhou, mas nenhum valor fora do dominio foi encontrado na consulta.",
                True, PROPOSTAS[FALHA_TESTE_DBT])
    partes, props = [], []
    for valor, n in valores:
        codigo = dominios.correspondencia(modelo, coluna, valor)
        partes.append(f"'{valor}' ({n} linha(s))")
        if codigo:
            props.append(f"tratar '{valor}' como sinonimo de '{codigo}' (normalizacao sem pontuacao e acentos) na Silver")
        else:
            props.append(f"'{valor}' sem correspondencia no dominio; confirmar com o dono do dado")
    resumo = f"Valores fora do dominio de {modelo}.{coluna}: " + ", ".join(partes) + "."
    proposta = "Proposta: " + "; ".join(props) + ". Alteracao via PR, sem liberar dados automaticamente."
    return DRIFT_DE_DOMINIO, resumo, True, proposta
