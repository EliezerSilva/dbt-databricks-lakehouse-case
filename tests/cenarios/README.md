# Cenários do agente de quarentena

Os cenários rodam em schemas isolados e descartáveis (`workspace.*_cen`), montados por
`montar.py` a partir de `montar_cenarios.sql`: cópias das tabelas originais, com as anomalias
G2 e G4 aplicadas por SQL. O script valida todos os statements antes de executar e só aceita
escrita em `*_cen`.

O pipeline DAB não é usado para os cenários: suas guardas de replay bloqueiam, por desenho,
o reprocessamento de fontes alteradas (comportamento confirmado em 2026-11-12).

| Cenário | Origem | Estado | Detecção esperada |
|---|---|---|---|
| G1 — descarte legítimo | real | região `XX` inativa em `tabelas_invalidas` | `DESCARTE_LEGITIMO` |
| G2 — drift de domínio | simulado | `SUL.` em `silver_regioes` e `dim_regiao` | dbt `accepted_values` → `DRIFT_DE_DOMINIO` |
| G3 — órfãos | real | 49 pedidos sem cabeçalho | `AUSENTE_NA_ORIGEM` |
| G4 — drift de layout | simulado | NOK de `vendedores.csv` no `ingestion_log` | `DRIFT_DE_LAYOUT` |
| G5 — duplicação por reexecução | real | `XX` gravada 3 vezes | 1 incidente com 3 ocorrências |

Os arquivos `g2_drift_dominio/` e `g4_drift_layout/` documentam a alteração de fonte que cada
cenário representa.

Execução:

    python tests/cenarios/montar.py
    cd dbt && dbt build --target cenarios --vars '{silver_schema: silver_cen, gold_schema: gold_cen}'
    cd .. && python -m agent.run --ambiente cenarios --dbt-target-dir dbt/target
