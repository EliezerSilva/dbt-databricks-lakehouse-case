# Camada semântica — Unity Catalog Metric Views

Métricas governadas definidas como Metric Views (YAML 1.1), versionadas no dbt com a materialização
nativa `metric_view` do dbt-databricks. As Metric Views aparecem no lineage do dbt (`ref()` para fato e dimensões).

| Objeto | Grão | Fonte |
|---|---|---|
| `dbt_gold.mv_vendas` | Item de pedido | `fato_pedidos` + dimensões |
| `dbt_gold.mv_pedidos` | Pedido | `fct_pedidos` (fato agregada por pedido) + dimensões |

Consulta: `SELECT canal, MEASURE(receita_liquida) FROM workspace.dbt_gold.mv_vendas GROUP BY canal`.

## Dimensões

Período (`ano_mes`, `ano`, `trimestre`), `categoria` e `subcategoria` (só vendas), `canal`, `tipo_canal`,
`regiao`, `segmento` e `status_pedido`. Valores sem atribuição aparecem explicitamente como `Sem canal`,
`Sem região` e `Não informado` — 12% da receita não tem canal atribuído na origem.

## Critério de atividade por grão

| Grão | Excluído das medidas | Motivo |
|---|---|---|
| Item (`mv_vendas`) | Itens cancelados (`flag_cancelado`: item ou pedido cancelado) | Regra do PySpark, mantida |
| Pedido (`mv_pedidos`) | Pedidos com status `CANCELADO` (ticket, descontos, `pedidos_ativos`) | Ticket e desconto são atributos do cabeçalho; o filtro coerente é o status do cabeçalho |

Dos 382 pedidos: 49 com status `CANCELADO`, 163 vigentes com algum item cancelado, 170 sem cancelamento.

## Medidas

**mv_vendas**: `receita_bruta`, `receita_liquida`, `quantidade`, `pedidos`, `clientes`, `preco_medio_vendido`.

**mv_pedidos**: `pedidos`, `pedidos_ativos`, `pedidos_com_cancelamento`, `taxa_cancelamento_pct`,
`pedidos_atrasados`, `taxa_atraso_pct`, `ticket_medio`, `descontos`, `ocorrencias`, `pedidos_com_ocorrencia`,
`pedidos_com_devolucao`, `pedidos_com_pedido_de_cancelamento`, `tempo_medio_entrega_dias`,
`media_dias_atraso`, `custo_frete`.

`pedidos_com_cancelamento` e `taxa_cancelamento_pct` mantêm a definição do PySpark (pedido com qualquer item
cancelado), reconciliada por teste.

## Reconciliação com o PySpark (testes, todos PASS)

| Teste | Contra | Medidas |
|---|---|---|
| `assert_mv_vendas_reconcilia_periodo` | `metricas_por_periodo` | receita bruta e líquida, pedidos, por mês |
| `assert_mv_vendas_reconcilia_canal` | `metricas_por_canal` | receitas, pedidos, clientes, por canal |
| `assert_mv_vendas_reconcilia_regiao` | `metricas_por_regiao` | receitas, pedidos, clientes, por região |
| `assert_mv_vendas_reconcilia_categoria` | `metricas_por_categoria` | receitas, quantidade, pedidos, preço médio |
| `assert_mv_pedidos_reconcilia_operacionais` | `metricas_operacionais` | pedidos, com cancelamento, atrasados e taxas, por mês × região × canal |
| `assert_atributos_pedido_constantes` | `fato_pedidos` | premissa do grão de pedido |

## Métricas corrigidas (divergência intencional)

O PySpark agrega atributos de pedido (repetidos em cada item da fato) no grão de item, o que causa
dupla contagem ou ponderação pelo número de itens. A camada semântica agrega esses atributos uma vez por pedido.

| Métrica | PySpark (grão de item) | Camada semântica (grão de pedido) |
|---|---|---|
| Ticket médio | R$ 7.206,25 | R$ 7.221,25 |
| Descontos | R$ 144.741,98 | R$ 87.137,74 |
| Ocorrências | 576 | 238 |
| Pedidos com ocorrência | 293 (itens) | 179 |
| Pedidos com devolução | 113 (itens) | 66 |
| Pedidos de cancelamento | 67 (itens) | 26 |
| Pedidos atrasados | 246 (itens) | 164 |
| Tempo médio de entrega (dias) | 6,06 | 5,96 |
| Média de dias de atraso | 129,00 | 138,73 |

Valores totais em 2026-11-12. PySpark recalculado com as fórmulas de `03_gold_metricas.py` e o filtro de
cada tabela de origem. A métrica `total_atrasos` tinha duas definições no PySpark (itens nas tabelas por
período/região/canal, pedidos na operacional); a camada semântica unifica em pedidos.

## Decisões

| Decisão | Escolha |
|---|---|
| Tecnologia | Unity Catalog Metric Views 1.1 (validadas no Free Edition em 2026-11-12) |
| Versionamento | Materialização `metric_view` do dbt-databricks, com `ref()` e lineage |
| `vw_pedidos_analytics` (PySpark) | Não migrada: substituída pelas Metric Views, que impedem somar colunas de pedido no grão de item |
| Nomes de joins | Prefixo `d_` para não colidir com nomes de dimensões (`canal`, `regiao`) |
