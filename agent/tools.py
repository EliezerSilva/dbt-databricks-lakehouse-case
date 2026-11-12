INVALIDOS = """
SELECT source_table, motivo, fp,
       any_value(payload) AS payload,
       count(*) AS ocorrencias,
       cast(min(processing_timestamp) AS STRING) AS primeira,
       cast(max(processing_timestamp) AS STRING) AS ultima
FROM (
    SELECT source_table, motivo, payload, processing_timestamp,
           sha2(regexp_replace(payload, '"processing_timestamp":"[^"]*",?', ''), 256) AS fp
    FROM workspace.silver.tabelas_invalidas
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
    FROM workspace.silver.tabelas_orfas
    GROUP BY source_table, join_key_name, join_key_value, payload
)
GROUP BY source_table, join_key_name, join_key_value
ORDER BY join_key_value
"""

PRESENCA_CABECALHO = """
SELECT o.join_key_value AS chave,
       max(CASE WHEN b.order_id IS NOT NULL THEN 1 ELSE 0 END) AS na_bronze,
       max(CASE WHEN s.order_id IS NOT NULL THEN 1 ELSE 0 END) AS na_silver
FROM (SELECT DISTINCT join_key_value FROM workspace.silver.tabelas_orfas WHERE join_key_name = 'order_id') o
LEFT JOIN workspace.bronze.bronze_erp_pedidos_cabecalho b ON b.order_id = o.join_key_value
LEFT JOIN workspace.silver.silver_pedidos_cabecalho s ON s.order_id = o.join_key_value
GROUP BY o.join_key_value
"""

CARGA_CABECALHO = """
SELECT source_file, target_table, status, records_loaded, validation_message,
       cast(pipeline_execution_timestamp AS STRING) AS executado_em
FROM workspace.control.ingestion_log
WHERE target_table LIKE '%bronze_erp_pedidos_cabecalho'
ORDER BY pipeline_execution_timestamp DESC
LIMIT 1
"""


def incidentes_invalidos(db):
    return db.read(INVALIDOS)


def incidentes_orfaos(db):
    return db.read(ORFAOS)


def presenca_cabecalho(db):
    return db.read(PRESENCA_CABECALHO)


def carga_cabecalho(db):
    rows = db.read(CARGA_CABECALHO)
    return rows[0] if rows else None
