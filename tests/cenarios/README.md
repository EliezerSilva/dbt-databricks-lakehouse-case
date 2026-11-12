# Cenários sintéticos do agente de quarentena

Cópias das fontes originais com uma anomalia controlada cada. Publicadas em
`/Volumes/workspace/cenarios/fontes/<cenario>/` junto com as demais fontes sem alteração,
e executadas somente no target `dev` do bundle (schemas `*_dev`).

| Cenário | Arquivo | Alteração | Detecção esperada |
|---|---|---|---|
| g2 — drift de domínio | `legado_regioes_pipe.txt` | `sul` → `Sul.` | dbt `accepted_values` em `dim_regiao.regional_code` |
| g4 — drift de layout | `vendedores.csv` | coluna `status` removida | NOK em `control_dev.ingestion_log` |
