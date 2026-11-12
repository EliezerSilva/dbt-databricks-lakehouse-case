# fato_pedidos — PySpark para dbt

## Escopo

| Papel | Objeto |
|---|---|
| Origens (não alteradas) | `silver_pedidos_cabecalho`, `silver_pedidos_itens`, `silver_logistica`, `silver_atendimento_ocorrencias` |
| Dimensões (dbt) | `dim_cliente`, `dim_produto`, `dim_vendedor`, `dim_canal`, `dim_regiao`, `dim_data` |
| Baseline PySpark (não alterada) | `gold.fato_pedidos` — `notebooks/gold/02_gold_fato_pedidos.py` |
| Mart dbt | `dbt_gold.fato_pedidos` (table) e `dbt_gold.itens_sem_cabecalho` (view) |

Grão: 1 linha por item de pedido com cabeçalho.

## Regras reproduzidas

| Elemento | Regra |
|---|---|
| Itens × cabeçalho | `left join` por `order_id`, mantendo itens com cabeçalho |
| Logística | Uma entrega por pedido: `delivered_at` mais recente, nulos por último; `delivery_id` como desempate final (adicionado para determinismo, sem efeito nos dados atuais) |
| Ocorrências | Por pedido: quantidade, `flag_cancel_request`, `flag_devolucao` (`refund`, `troca`), `flag_tem_ocorrencia` |
| `flag_cancelado` | `status_order` = CANCELADO ou `item_status` = CANCELADO |
| `receita_liquida_item` | `round(valor_item - desconto_pedido × valor_item / valor_bruto_pedido, 2)` quando `valor_bruto_pedido > 0`; senão `valor_item` |
| SKs de canal e região | Derivadas do vendedor, não do pedido |
| SKs de data | Lookup em `dim_data` pela data do pedido e pela data prevista |

## Diferenças de desenho em relação ao PySpark

| Aspecto | PySpark | dbt | Efeito no conteúdo |
|---|---|---|---|
| Itens sem cabeçalho | Gravados em append em `silver.tabelas_orfas` e `silver.quality_log` durante a construção da fato | View `itens_sem_cabecalho` e teste `assert_itens_sem_cabecalho` (warn), sem efeito colateral | Nenhum |
| Particionamento | `partitionBy(status_order)` | Sem particionamento (912 linhas) | Nenhum |
| Grão | Verificado por `print` | Teste `unique_combination(order_id, item_seq)` | Nenhum |

A quarentena PySpark continua existindo e alimentando o agente de quarentena.

## Baseline

A fato PySpark (v1, 2026-05-09 03:45) é posterior à reescrita do cabeçalho (v2, 03:42; 400 pedidos).
A Silver de itens foi reescrita nas versões 16 a 19 (junho a outubro de 2026) com as mesmas 992 linhas;
a reconciliação confirma que o conteúdo não mudou.

## Resultado — execução de 2026-11-12

| Verificação | dbt | PySpark |
|---|---|---|
| Linhas | 912 | 912 |
| Pedidos | 382 | 382 |
| Receita líquida | R$ 4.926.024,09 | R$ 4.926.024,09 |
| `reconciles_with` (33 colunas) | PASS | — |

`dbt build` do projeto: `PASS=106 WARN=18 ERROR=0`.

## Cobertura de chaves (impacto para a camada semântica)

| SK nula | Itens | Causa |
|---|---|---|
| `sk_canal` | 109 (12%) | 5 vendedores sem canal |
| `sk_regiao` | 61 (7%) | 3 vendedores com região `NAO_INFORMADO` |
| `sk_cliente` | 20 (2%) | Cliente placeholder `C9999` |
| `custo_entrega` não numérico | 20 | 10 entregas com custo inválido |
| Itens sem cabeçalho (fora da fato) | 80 itens, 49 pedidos | Ver agente de quarentena (G3) |

Métricas por canal e por região precisam de categoria explícita para valores sem atribuição.
