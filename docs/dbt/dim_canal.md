# dim_canal — PySpark para dbt

## Escopo

| Papel | Objeto |
|---|---|
| Origem (não alterada) | `workspace.silver.silver_canais` |
| Baseline PySpark (não alterada) | `workspace.gold.dim_canal` — `notebooks/gold/01_gold_dimensoes.py` |
| Staging dbt | `workspace.dbt_staging.stg_canais` (view) |
| Mart dbt | `workspace.dbt_gold.dim_canal` (table, Delta) |

Lineage: `source(silver.silver_canais)` → `stg_canais` → `dim_canal`.
A Gold PySpark é declarada como source `pyspark_gold` e usada apenas no teste de reconciliação.

## Mapeamento

| Coluna | Regra | Equivalência com PySpark |
|---|---|---|
| `sk_canal` | `sha2(canal_id, 256)` sobre o valor cru | Idêntica |
| `canal_id` | `id_canal` renomeado em `stg_canais` | Idêntica |
| `nome_canal`, `tipo_canal`, `ativo` | Repassados da Silver | Idêntica |

## Diferença semântica: deduplicação

A Silver contém `CH05` duplicado com atributos divergentes (`E-commerce` e `ecommerce`).

- PySpark: `dropDuplicates(["canal_id"])` — não determinístico; a linha mantida não é garantida.
- dbt: `row_number()` por `canal_id`, ordenado por `nome_canal asc nulls last, tipo_canal asc nulls last, ativo desc nulls last` — determinístico.

Com os dados atuais, as duas regras produzem `E-commerce` (ordenação `UTF8_BINARY`: `E` < `e`).
A paridade atual decorre da ordenação, não de equivalência formal entre as regras.

## Testes — execução de 2026-10-16

| Teste | Severidade | Resultado |
|---|---|---|
| `unique` / `not_null` em `sk_canal` | error | PASS |
| `unique` / `not_null` em `canal_id` | error | PASS |
| `accepted_values` em `tipo_canal` (Direto, Indireto, Digital) | error | PASS |
| `not_null` em `nome_canal` | warn | WARN 1 (`CH06`) |
| `not_null` em `ativo` | warn | WARN 1 (`ch07`) |
| `not_null` em `id_canal` (source) | error | PASS |
| `unique` em `id_canal` (source) | warn | WARN 1 (`CH05`) |
| `assert_dim_canal_reconciles_with_pyspark` | error | PASS |

Resultado: `PASS=7 WARN=3 ERROR=0`.

## Reconciliação

Método: `EXCEPT` nos dois sentidos entre `dbt_gold.dim_canal` e `gold.dim_canal`, sobre as 5 colunas.

- Diferenças: 0 em ambos os sentidos
- Linhas: 7 e 7
- Baseline válido: a Silver não foi alterada após a construção da Gold PySpark (histórico Delta)
- Gold PySpark permaneceu na versão 0 após a execução do dbt

## Issues de qualidade herdadas da origem

Não corrigidas no dbt, para preservar paridade de chaves com `fato_pedidos`, `metricas_por_canal` e `vw_kpis_pedidos`.

| Registro | Problema | Impacto |
|---|---|---|
| `CH05` | Duplicado com grafias divergentes | Resolvido por regra determinística na dimensão |
| `CH06` | `nome_canal` nulo | Atributo ausente no consumo |
| `ch07` | ID fora do padrão `CHnn` e `ativo` nulo | `vendedores.csv` usa o mesmo `ch07`; normalizar só a dimensão alteraria `sk_canal` e quebraria o join da fato |

A correção deve ocorrer na Silver ou na origem, de forma coordenada entre dimensão e fato.

## Decisões

| Decisão | Escolha |
|---|---|
| Schemas | `dbt_staging` e `dbt_gold`, via `generate_schema_name` padrão (`schema: dbt` + `+schema`) |
| Materialização | `view` em staging; `table` no mart, equivalente ao overwrite original |
| Autenticação | OAuth U2M; nenhum token em arquivo |
| Concorrência | `threads: 1`, para limitar consumo no Free Edition |
