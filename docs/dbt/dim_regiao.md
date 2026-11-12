# dim_regiao — PySpark para dbt

## Escopo

| Papel | Objeto |
|---|---|
| Origem (não alterada) | `workspace.silver.silver_regioes` |
| Baseline PySpark (não alterada) | `workspace.gold.dim_regiao` — `notebooks/gold/01_gold_dimensoes.py` |
| Staging dbt | `workspace.dbt_staging.stg_regioes` (view) |
| Mart dbt | `workspace.dbt_gold.dim_regiao` (table, Delta) |

## Mapeamento

| Coluna Gold | Origem Silver | Camada da renomeação |
|---|---|---|
| `sk_regiao` | `sha2(regional_code, 256)` | mart |
| `regional_code` | `regional_code` | — |
| `nome_regiao` | `regional_name` | staging |
| `estado_sede` | `state` | staging |
| `gestor` | `manager_name` | staging |

## Diferença semântica: deduplicação

A Silver PySpark já entrega uma linha por `regional_code`: normaliza `sul` para `S`, descarta inativas e sem nome, e resolve `SE` duplicado com `row_number` preferindo `state = SP`.

- PySpark Gold: `dropDuplicates(["regional_code"])` — sem efeito nos dados atuais.
- dbt: sem deduplicação. A unicidade é um teste `unique` com severidade `error` na source. Se a Silver deixar de ser única, o build falha em vez de escolher uma linha arbitrária.

Limitação herdada (não corrigida, Silver segue em PySpark): a janela da Silver só desempata `state = SP`. Dois registros não-SP para o mesmo código teriam escolha não determinística.

## Testes — execução de 2026-11-12

| Teste | Severidade | Resultado |
|---|---|---|
| `unique` / `not_null` em `regional_code` (source) | error | PASS |
| `unique` / `not_null` em `sk_regiao` e `regional_code` | error | PASS |
| `accepted_values` em `regional_code` (CO, N, NE, S, SE) | error | PASS |
| `not_null` em `nome_regiao` (garantido pelo filtro da Silver) | error | PASS |
| `not_null` em `estado_sede`, `gestor` | warn | PASS |
| `reconciles_with` contra `pyspark_gold.dim_regiao` | error | PASS |

`dbt build` do projeto: `PASS=22 WARN=3 ERROR=0` (os 3 WARN são de `dim_canal`, documentados em `dim_canal.md`).

## Reconciliação

Teste genérico `reconciles_with` (`dbt/tests/generic/reconciles_with.sql`): `EXCEPT` nos dois sentidos sobre as 5 colunas.

- Diferenças: 0
- Linhas: 5 e 5
- Baseline válido: Silver v2 (2026-05-09 02:10) anterior à Gold v0 (02:43)
- Gold PySpark permaneceu na versão 0

## Consumidores

`fato_pedidos` resolve `sk_regiao` por `regional_code`; `metricas_por_regiao` e `vw_kpis_pedidos` fazem join por `sk_regiao`. Paridade bit a bit de `sk_regiao` confirmada pela reconciliação.
