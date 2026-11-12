# dim_vendedor — PySpark para dbt

## Escopo

| Papel | Objeto |
|---|---|
| Origem (não alterada) | `workspace.silver.silver_vendedores` |
| Baseline PySpark (não alterada) | `workspace.gold.dim_vendedor` — `notebooks/gold/01_gold_dimensoes.py` |
| Staging dbt | `workspace.dbt_staging.stg_vendedores` (view) |
| Mart dbt | `workspace.dbt_gold.dim_vendedor` (table, Delta) |

Lineage: `source(silver.silver_vendedores)` → `stg_vendedores` → `dim_vendedor` ← `dim_canal`.

## Mapeamento

| Coluna Gold | Origem Silver | Camada da renomeação |
|---|---|---|
| `sk_vendedor` | `sha2(seller_id, 256)` | mart |
| `seller_id`, `canal_id`, `regional_code`, `status` | mesmas colunas | — |
| `nome_vendedor` | `seller_name` | staging |
| `data_admissao` | `hire_date` | staging |

A Silver já normaliza `regional_code` (`sul` → `S`, nulo → `NAO_INFORMADO`) e `status` (nulo → `NAO_INFORMADO`, `initcap`).

## Diferença semântica: deduplicação

A Silver não deduplica: 42 linhas para 40 vendedores. Duplicatas com atributos divergentes:

| seller_id | Divergência | Mantida pela Gold PySpark |
|---|---|---|
| V004 | `canal_id` `CH02` / `CH99` | `CH02` |
| V008 | `seller_name` "Vendedor 8" / "Vendedor 8 duplicado" | "Vendedor 8" |

- PySpark: `dropDuplicates(["seller_id"])` — não determinístico.
- dbt: `row_number()` por `seller_id`, preferindo o registro cujo `canal_id` existe em `dim_canal`; desempate por `nome_vendedor` crescente.

O primeiro critério tem significado de negócio (integridade referencial). O segundo reproduz a Gold atual por ordenação, não por equivalência formal.

## Testes — execução de 2026-11-12

| Teste | Severidade | Resultado |
|---|---|---|
| `not_null` em `seller_id` (source) | error | PASS |
| `unique` em `seller_id` (source) | warn | WARN 2 (V004, V008) |
| `unique` / `not_null` em `sk_vendedor` e `seller_id` | error | PASS |
| `not_null` em `nome_vendedor` | error | PASS |
| `not_null` em `regional_code` | error | PASS |
| `relationships` `regional_code` → `dim_regiao` (exceto `NAO_INFORMADO`) | error | PASS |
| `relationships` `canal_id` → `dim_canal` | error | PASS |
| `not_null` em `canal_id` | warn | WARN 5 |
| `not_null` em `data_admissao` | warn | PASS (0 nulos; protege contra formato de data não previsto na Silver) |
| `accepted_values` em `status` (Ativo, Inativo, NAO_INFORMADO) | error | PASS |
| `reconciles_with` contra `pyspark_gold.dim_vendedor` | error | PASS |

`dbt build` do projeto: `PASS=36 WARN=5 ERROR=0` (6 modelos, 35 testes).

## Reconciliação

- Diferenças: 0; linhas: 40 e 40
- Baseline válido: Silver v2 (2026-05-09 02:10) anterior à Gold v0 (02:43)
- Gold PySpark permaneceu na versão 0

## Consumidores

`fato_pedidos` busca `sk_vendedor`, `canal_id` e `regional_code` por `seller_id` — as SKs de canal e região da fato dependem desta dimensão. `metricas_por_canal` e `vw_kpis_pedidos` fazem join por `sk_vendedor`.

## Qualidade herdada da origem

| Item | Registros | Tratamento |
|---|---|---|
| `seller_id` duplicado com atributos divergentes | V004, V008 | Regra determinística na dimensão |
| Canal inexistente `CH99` | linha descartada de V004 | Eliminado pela regra (canal válido primeiro) |
| Sem canal | V007, V011, V021, V027, V035 | WARN; vendas desses vendedores ficam sem canal na fato |
| Região `NAO_INFORMADO` | V018, V022, V024 | Sentinela da Silver, excluído do teste de relacionamento |
| `canal_id` `ch07` fora do padrão | V024 e outros 3 | Consistente com `dim_canal`; ver `dim_canal.md` |
