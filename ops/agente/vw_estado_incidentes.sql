CREATE OR REPLACE VIEW workspace.agent.vw_estado_incidentes AS
SELECT ambiente, incidente_id, fonte, status_resultante AS status, classificacao, source_table,
       deteccoes, reaberturas, primeira, ultima
FROM (
  SELECT *,
         row_number() OVER (PARTITION BY ambiente, incidente_id ORDER BY seq DESC) AS rn,
         count(CASE WHEN evento IN ('NOVO', 'DETECTADO', 'REABERTO') THEN 1 END) OVER (PARTITION BY ambiente, incidente_id) AS deteccoes,
         count(CASE WHEN evento = 'REABERTO' THEN 1 END) OVER (PARTITION BY ambiente, incidente_id) AS reaberturas,
         min(criado_em) OVER (PARTITION BY ambiente, incidente_id) AS primeira,
         max(criado_em) OVER (PARTITION BY ambiente, incidente_id) AS ultima
  FROM workspace.agent.incidentes_eventos
)
WHERE rn = 1
