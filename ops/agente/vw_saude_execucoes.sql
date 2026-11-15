CREATE OR REPLACE VIEW workspace.agent.vw_saude_execucoes AS
WITH ev AS (
  SELECT agent_run_id, ambiente, min(criado_em) AS quando, count(*) AS eventos,
         count_if(evento = 'NOVO') AS novos, count_if(evento = 'REABERTO') AS reabertos,
         count_if(evento = 'RESOLVIDO') AS resolvidos
  FROM workspace.agent.incidentes_eventos
  WHERE agent_run_id IS NOT NULL
  GROUP BY agent_run_id, ambiente
),
fe AS (
  SELECT agent_run_id, count(*) AS chamadas_ferramenta, count_if(status <> 'OK') AS chamadas_com_problema
  FROM workspace.agent.chamadas_ferramentas
  GROUP BY agent_run_id
),
na AS (
  SELECT agent_run_id, count(*) AS narrativas, count_if(llm_status = 'OK') AS narrativas_ok,
         count_if(llm_status NOT IN ('OK', 'SUPRIMIDO_RECONHECIDO', 'DESATIVADO')) AS narrativas_com_problema
  FROM workspace.agent.narrativas
  GROUP BY agent_run_id
)
SELECT ev.*, coalesce(fe.chamadas_ferramenta, 0) AS chamadas_ferramenta,
       coalesce(fe.chamadas_com_problema, 0) AS chamadas_com_problema,
       coalesce(na.narrativas, 0) AS narrativas, coalesce(na.narrativas_ok, 0) AS narrativas_ok,
       coalesce(na.narrativas_com_problema, 0) AS narrativas_com_problema
FROM ev LEFT JOIN fe USING (agent_run_id) LEFT JOIN na USING (agent_run_id)
