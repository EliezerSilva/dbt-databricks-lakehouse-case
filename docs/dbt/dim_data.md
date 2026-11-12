# dim_data — PySpark para dbt

| Papel | Objeto |
|---|---|
| Origens (não alteradas) | `silver.silver_pedidos_cabecalho`, `silver.silver_logistica` |
| Baseline PySpark (não alterada) | `gold.dim_data` — `notebooks/gold/01_gold_dimensoes.py` |
| Staging dbt | `stg_pedidos_cabecalho`, `stg_logistica` (views, sem renomeação; reutilizadas pela fato) |
| Mart dbt | `dbt_gold.dim_data` (table, Delta) |

## Regra

Range: menor e maior data entre `order_date` e `promised_date` (pedidos) e `shipped_at` e `delivered_at`
convertidos para data (entregas), estendido em 30 dias para cada lado. Sequência diária via `sequence` em SQL,
equivalente ao `explode(sequence(...))` do PySpark. Atributos com as mesmas funções nativas (`weekofyear` ISO,
`quarter`, `dayofweek`); nomes de mês e de dia em português; `dia_semana_iso` com segunda = 1 e domingo = 7.

## Baseline

A Silver de pedidos foi reescrita (v2, 400 linhas) depois da Gold (`dim_data` v0). A dimensão depende apenas dos
limites do range, que permanecem iguais: 2024-12-04 a 2026-04-08, 491 dias. Baseline válido.

## Testes — execução de 2026-11-12

| Teste | Severidade | Resultado |
|---|---|---|
| `unique` / `not_null` em `data` e `sk_data` | error | PASS |
| `accepted_values` em `dia_semana_iso`, `flag_fim_semana`, `nome_mes`, `nome_dia_semana` | error | PASS |
| Contiguidade (`assert_dim_data_contigua`) | error | PASS |
| `reconciles_with` contra `pyspark_gold.dim_data` (12 colunas) | error | PASS |

## Observação para a fase 2

`silver_pedidos_cabecalho` passou de 403 (v1) para 400 linhas (v2, 03:42 de 2026-05-09), depois da construção
das dimensões. A reconciliação da `fato_pedidos` precisa verificar sobre qual versão a Gold da fato foi construída.
