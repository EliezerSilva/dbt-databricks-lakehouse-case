# Ontologia

Fonte única do vocabulário do domínio comercial: `ontologia/ontologia.yml`, versionada no repositório e lida por
`agent/ontologia.py`.

| Seção | Conteúdo |
|---|---|
| `entidades` | Item de pedido, pedido, cliente, produto, vendedor, canal, região e data: descrição, modelo dbt, chave, tabela Silver de origem e dimensões da camada semântica |
| `relacoes` | Ligações entre entidades e a coluna que as realiza (ex.: vendedor → canal via `canal_id`) |
| `atribuicao` | Cadeia pela qual cada dimensão chega à receita (ex.: canal da venda = canal do vendedor do pedido), nome, gênero e rótulo de lacuna |
| `dominios` | Códigos válidos e sinônimos (antes em `agent/dominios.py`, que agora lê daqui com a mesma interface) |
| `metricas` | Métricas de negócio, Metric View, grão e definição |

## Consumidores

| Consumidor | Uso |
|---|---|
| Agente: incidentes de lacuna | Rótulo e concordância do resumo; cadeia de atribuição no `detalhar_incidente` |
| Agente: `amostrar_registros` de lacuna | Consulta gerada da cadeia: registros do último elo sem correspondência (identificadores validados) |
| Agente: domínios | Classificação de drift de domínio e sinônimos |
| CI | `python -m agent.ontologia --manifest dbt/target/manifest.json`: modelos, dimensões e medidas citados precisam existir; ontologia desatualizada quebra o PR |

## Evidência (2026-11-13, ambiente `original`)

| Lacuna | Cadeia | Registros identificados |
|---|---|---|
| Canal, 15,6% da receita | item → pedido (`order_id`) → vendedor (`seller_id`) → canal (`canal_id`) | V007, V011, V021, V027, V035: os vendedores sem canal levantados na construção da `dim_vendedor` |
| Região, 4,5% | item → pedido → vendedor → região (`regional_code`) | 3 vendedores com o sentinela `NAO_INFORMADO` |
| Segmento, 20,2% | item → pedido → cliente (`customer_code`), atributo `segmento` | Clientes cadastrados sem segmento |

## Limitações

- Nomes de colunas são validados no banco apenas na execução; o manifest só documenta colunas declaradas em YAML.
- A causa listada é a do último elo da cadeia. Lacunas em elos anteriores (ex.: pedido de cliente inexistente no
  cadastro, como o placeholder `C9999`) contribuem para o valor da lacuna e não são listadas.
- Escopo: domínio comercial atual. Genie e BI fora de escopo.
