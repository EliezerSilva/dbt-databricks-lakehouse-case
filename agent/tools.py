AMBIENTES = {
    "original": {"bronze": "workspace.bronze", "silver": "workspace.silver", "control": "workspace.control"},
    "dev": {"bronze": "workspace.bronze_dev", "silver": "workspace.silver_dev", "control": "workspace.control_dev"},
    "cenarios": {"bronze": "workspace.bronze_cen", "silver": "workspace.silver_cen", "control": "workspace.control_cen"},
}

INVALIDOS = """
SELECT source_table, motivo, fp,
       any_value(payload) AS payload,
       count(*) AS ocorrencias,
       cast(min(processing_timestamp) AS STRING) AS primeira,
       cast(max(processing_timestamp) AS STRING) AS ultima
FROM (
    SELECT source_table, motivo, payload, processing_timestamp,
           sha2(regexp_replace(payload, '"processing_timestamp":"[^"]*",?', ''), 256) AS fp
    FROM {silver}.tabelas_invalidas
)
GROUP BY source_table, motivo, fp
ORDER BY source_table, motivo
"""

ORFAOS = """
SELECT source_table, join_key_name, join_key_value,
       sum(n) AS ocorrencias,
       count(*) AS registros,
       round(sum(cast(get_json_object(payload, '$.total_item') AS DOUBLE)), 2) AS valor,
       cast(min(primeira) AS STRING) AS primeira,
       cast(max(ultima) AS STRING) AS ultima
FROM (
    SELECT source_table, join_key_name, join_key_value, payload,
           count(*) AS n, min(processing_timestamp) AS primeira, max(processing_timestamp) AS ultima
    FROM {silver}.tabelas_orfas
    GROUP BY source_table, join_key_name, join_key_value, payload
)
GROUP BY source_table, join_key_name, join_key_value
ORDER BY join_key_value
"""

PRESENCA_CABECALHO = """
SELECT o.join_key_value AS chave,
       max(CASE WHEN b.order_id IS NOT NULL THEN 1 ELSE 0 END) AS na_bronze,
       max(CASE WHEN s.order_id IS NOT NULL THEN 1 ELSE 0 END) AS na_silver
FROM (SELECT DISTINCT join_key_value FROM {silver}.tabelas_orfas WHERE join_key_name = 'order_id') o
LEFT JOIN {bronze}.bronze_erp_pedidos_cabecalho b ON b.order_id = o.join_key_value
LEFT JOIN {silver}.silver_pedidos_cabecalho s ON s.order_id = o.join_key_value
GROUP BY o.join_key_value
"""

_CARGA_COLS = """
SELECT source_file, target_table, status, records_loaded, validation_message,
       cast(pipeline_execution_timestamp AS STRING) AS executado_em
FROM {control}.ingestion_log
WHERE target_table LIKE '%bronze_erp_pedidos_cabecalho'
"""

CARGA_ANTERIOR = _CARGA_COLS + """  AND pipeline_execution_timestamp <= cast(:antes AS TIMESTAMP)
ORDER BY pipeline_execution_timestamp DESC
LIMIT 1
"""

CARGA_RECENTE = _CARGA_COLS + """ORDER BY pipeline_execution_timestamp DESC
LIMIT 1
"""

LAYOUT_NOK = """
SELECT source_file, target_table, status, records_loaded, validation_message,
       detected_schema, expected_schema, cast(pipeline_execution_timestamp AS STRING) AS executado_em
FROM (
    SELECT *, row_number() OVER (PARTITION BY source_file ORDER BY pipeline_execution_timestamp DESC) AS rn
    FROM {control}.ingestion_log
)
WHERE rn = 1 AND status = 'NOK'
ORDER BY source_file
"""

VALORES_FORA = """
SELECT cast({col} AS STRING) AS valor, count(*) AS n
FROM {rel}
WHERE {col} IS NOT NULL AND NOT array_contains(from_json(:valores, 'array<string>'), cast({col} AS STRING))
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20
"""

TEMPLATES_AMBIENTE = (INVALIDOS, ORFAOS, PRESENCA_CABECALHO, CARGA_ANTERIOR, CARGA_RECENTE, LAYOUT_NOK)


def sql(template, amb):
    return template.format(**AMBIENTES[amb])


def incidentes_invalidos(db, amb):
    return db.read(sql(INVALIDOS, amb))


def incidentes_orfaos(db, amb):
    return db.read(sql(ORFAOS, amb))


def presenca_cabecalho(db, amb):
    return db.read(sql(PRESENCA_CABECALHO, amb))


def carga_cabecalho(db, amb, antes):
    rows = db.read(sql(CARGA_ANTERIOR, amb), {"antes": antes})
    if rows:
        return {**rows[0], "recorte": "anterior_aos_orfaos"}
    rows = db.read(sql(CARGA_RECENTE, amb))
    return {**rows[0], "recorte": "mais_recente"} if rows else None


def layout_nok(db, amb):
    return db.read(sql(LAYOUT_NOK, amb))


def valores_fora(db, relacao, coluna, valores):
    import json
    return db.read(VALORES_FORA.format(rel=relacao, col=coluna), {"valores": json.dumps([str(v) for v in valores])})
