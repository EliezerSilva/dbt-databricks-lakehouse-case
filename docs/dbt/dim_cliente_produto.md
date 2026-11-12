# dim_cliente e dim_produto — PySpark para dbt

## Escopo

| Papel | Clientes | Produtos |
|---|---|---|
| Origem (não alterada) | `silver.silver_clientes` | `silver.silver_produtos` |
| Baseline PySpark (não alterada) | `gold.dim_cliente` | `gold.dim_produto` |
| Staging dbt | `dbt_staging.stg_clientes` | `dbt_staging.stg_produtos` |
| Mart dbt | `dbt_gold.dim_cliente` | `dbt_gold.dim_produto` |

SKs: `sha2(customer_id, 256)` e `sha2(product_id, 256)` sobre o valor cru (paridade confirmada).

## Diferença semântica intencional: SCD1 pela versão mais recente

A Silver não deduplica. O `dropDuplicates` da Gold PySpark manteve a primeira ocorrência de cada chave, que nestes dados é a versão mais antiga:

| Chave | Versão mantida pelo PySpark | Versão vigente (dbt) |
|---|---|---|
| C0010 | segmento nulo (2026-01-09) | segmento Financeiro (2026-01-10) |
| C0025 | estado RJ (2025-05-07) | estado SC (2025-08-01) |
| P0006 | "Produto 6" (2025-11-02) | "Produto 6 revisado" (2026-02-12) |
| c0051 | e-mail válido (empate de `updated_at`) | e-mail válido (desempate por formato) |

Regra no dbt: `row_number()` por chave, `updated_at` decrescente; em clientes, desempate por e-mail com `@`.
A Gold PySpark está desatualizada nesses três registros; o dbt corrige.

## Reconciliação com divergências declaradas

As divergências intencionais ficam em `dbt/seeds/reconciliacao_excecoes.csv` (modelo, chave, motivo).
O teste genérico `reconciles_with` com `exceptions` e `key_column`:

- falha para qualquer diferença cuja chave não esteja declarada;
- falha para exceção declarada que não diverge mais (`declarada_sem_diferenca`).

Prova executada em 2026-11-12: com a exceção fictícia `C0001`, o teste falhou;
sem a exceção `C0025`, o teste falhou; com o seed restaurado, passou. Sem `exceptions`, o teste mantém o comportamento original (`dim_canal`, `dim_regiao`, `dim_vendedor`).

Impacto a jusante (fase 3): métricas por segmento, estado e produto herdam as mesmas três exceções na reconciliação com o PySpark.

## Testes

| Alvo | Teste | Severidade | Resultado |
|---|---|---|---|
| source `customer_id` / `product_id` | `not_null` / `unique` | error / warn | PASS / WARN 3 e 1 |
| source `updated_at` | `not_null` | warn | PASS |
| SKs e chaves | `unique`, `not_null` | error | PASS |
| `estado` | `not_null`, `accepted_values` (27 UFs) | error | PASS |
| `segmento`, `porte` | `accepted_values` / `not_null` | error / warn | PASS / WARN 32 e 39 |
| `status_cliente`, `status` (produto) | `accepted_values` | error | PASS |
| `email` | `not_null`, `like_pattern '%@%'` | warn | WARN 4 e 6 |
| `categoria`, `currency` (BRL) | `accepted_values` | error | PASS |
| `nome_produto` | `not_null` | warn | WARN 1 (P9999) |
| `preco_lista` | `castable_to_double` | warn | WARN 1 (P9999, `N/A`) |
| modelos | `reconciles_with` com exceções | error | PASS |

`dbt build` do projeto: `PASS=66 WARN=13 ERROR=0` (1 seed, 10 modelos).

## Qualidade herdada da origem

| Item | Registros | Observação |
|---|---|---|
| Placeholder de produto | P9999 (sem nome, preço `N/A`) | `preco_lista` é STRING; a conversão numérica fica para a camada semântica |
| Placeholder de cliente | C9999, em 8 pedidos, ausente do cadastro | Provável causa de `itens_sem_sk_cliente` na fato (fase 2) |
| E-mails sem `@` | C0029, C0058, C0087, C0116, C0145, C0174 | — |
| E-mails nulos | C0037, C0074, C0111, C0148 | — |
| `customer_id` fora do padrão | c0051 | Consistente com os pedidos (mesmo case) |
| Estados presentes | MG, PR, RJ, SC, SP | Domínio de teste: 27 UFs |
