# dbt + Databricks Lakehouse Case

Este repositório acompanha a evolução do projeto
[Databricks Lakehouse Case](https://github.com/EliezerSilva/databricks-lakehouse-case),
originalmente desenvolvido com PySpark, Delta Lake e arquitetura Medallion.

## Estado atual

A implementação original está preservada neste repositório como base de referência.
O ambiente local de desenvolvimento com dbt Core e dbt-databricks foi preparado.

A implementação dos modelos SQL com dbt, testes automatizados, documentação
e reconciliação com as tabelas originais está em desenvolvimento.

## Objetivo da modernização

Introduzir dbt nas transformações relacionais das camadas Silver e Gold,
mantendo PySpark nas etapas de ingestão e nos processamentos em que seu uso
for tecnicamente justificado.

As novas transformações serão validadas contra os resultados existentes
antes de qualquer substituição.

---

## Documentação da implementação original

# Lakehouse Case — Engenharia de Dados

Case técnico sobre Databricks Community Edition. Cobre ingestão, tratamento e modelagem analítica a partir de 9 fontes heterogêneas.

---

## Arquitetura

```
SOURCES (9 arquivos)
    │
    ├─────────────────────────────────► CONTROL  (ingestion_log)
    ▼
BRONZE   — ingestão raw, schema explícito, colunas técnicas
    │
    ▼
SILVER   — limpeza, normalização, deduplicação, isolamento de inválidos/órfãos
    │
    ▼
GOLD     — modelo dimensional + métricas pré-agregadas + view de consumo
    │
    ▼
BI       — Databricks SQL conectado ao Gold
```

Control é escrito pelos notebooks Bronze. Não participa do fluxo analítico.
Cada camada é reprocessável de forma independente.

---

## Estrutura

```
notebooks/
├── 00_setup.py
├── bronze/
│   ├── 01_bronze_pedidos_cabecalho.py
│   ├── 02_bronze_pedidos_itens.py
│   ├── 03_bronze_cadastros.py          # clientes, canais, vendedores, regioes
│   └── 04_bronze_json_sources.py       # produtos, entregas, ocorrencias
├── silver/
│   ├── 01_silver_pedidos_cabecalho.py
│   ├── 02_silver_pedidos_itens.py
│   ├── 03_silver_cadastros.py
│   └── 04_silver_json_sources.py
└── gold/
    ├── 01_gold_dimensoes.py
    ├── 02_gold_fato_pedidos.py
    ├── 03_gold_metricas.py
    └── 04_gold_vw_kpis_pedidos.py

tests/
├── validacoes_pos_carga.py
└── data_quality_checks.sql

docs/
├── qualidade_dados.md
└── resumo_executivo.md

sql/
└── consultas_bi.sql
```

---

## Execução

Requisitos: Databricks Community Edition (DBR 12+), arquivos em `/FileStore/case/sources/`.

```bash
# upload via CLI
databricks fs cp sources/ dbfs:/FileStore/case/sources/ --recursive
```

Ordem de execução:
```
00_setup
  → bronze/01 → 02 → 03 → 04
    → silver/01 → 02 → 03 → 04
      → gold/01 → 02 → 03 → 04
        → tests/validacoes_pos_carga
```

Cada notebook é autocontido. O `%run ../00_setup` no topo injeta os helpers e paths.

---

## Fontes

| Arquivo | Formato | Registros | Sep |
|---|---|---|---|
| erp_pedidos_cabecalho_2025.csv | CSV | 403 | `;` |
| erp_pedidos_itens_2025.csv | CSV | 995 | `,` |
| crm_clientes_export.xlsx | Excel | 183 | — |
| comercial_canais.xlsx | Excel | 8 | — |
| vendedores.csv | CSV | 42 | `;` |
| legado_regioes_pipe.txt | TXT | 8 | `\|` |
| cadastro_produtos_api_dump.json | JSON array | 72 | — |
| logistica_entregas.json | JSON array | 322 | — |
| atendimento_ocorrencias.ndjson | NDJSON | 270 | — |

---

## Modelo dimensional (Gold)

**Fato:** `fato_pedidos` — 1 linha por item de pedido

**Dimensões:** `dim_cliente`, `dim_produto`, `dim_canal`, `dim_regiao`, `dim_vendedor`, `dim_data`

**Métricas pré-agregadas:** `metricas_por_periodo`, `metricas_por_regiao`, `metricas_por_canal`, `metricas_por_categoria`, `metricas_operacionais`

**Consumo:** `gold.vw_kpis_pedidos` — view que resolve todas as dimensões, filtra cancelados. Ponto único de entrada para o time de BI.

---

## Problemas encontrados nos dados

| Fonte | Problema | Tratamento |
|---|---|---|
| pedidos_cabecalho | status_order nulo (64 registros) | → `DESCONHECIDO` |
| pedidos_cabecalho | 3 formatos de data distintos | coalesce com 3 padrões |
| pedidos_itens | item_status nulo (26%) | → `NAO_INFORMADO` |
| pedidos_itens | total_item divergente de quantity * unit_price | recomputado (tolerância 0.01) |
| entregas | delivery_status mix PT/EN | mapeamento explícito |
| regioes | código `XX` sem dados; `sul` ≠ `S` | descarte; normalização de alias |
| vendedores | regional_code = `sul` | → `S` |
| clientes | estado com nomes completos e siglas | normalização via `.isin()` |
| ocorrencias | event_type e severity nulos (14–22%) | → `NAO_INFORMADO` |

---

## Decisões técnicas

**Surrogate keys via `sha2`:** determinístico no reprocessamento, sem dependência de sequência de inserção.

**`receita_liquida_item`:** desconto fica no cabeçalho. Rateado proporcionalmente ao valor do item para evitar dupla contagem ao agregar por linha. Colunas de nível pedido (`valor_liquido_pedido`, `custo_entrega`) permanecem na fato para referência — agregar via `COUNT DISTINCT order_id`.

**Split Silver pedidos_cabecalho / pedidos_itens:** o join entre as duas tabelas ocorre no Gold (fato), não na Silver. Mantém granularidade e separação de responsabilidade entre camadas.

**`dim_data` dinâmica:** gerada do range efetivo dos dados ±30 dias. Sem hardcode de período.

**`xlsx` via pandas:** Spark não lê Excel no Community Edition. Decisão pragmática — em produção a conversão ficaria na landing zone.

**Particionamento por `status_order`:** adequado ao volume do case. Em produção, a partição natural seria por `ano_mes` de `order_date`.

---

## Premissas

- Pedidos com status_order nulo não são tratados como cancelados — estado desconhecido.
- Um pedido tem no máximo uma entrega ativa (mantida a mais recente por `delivered_at`).
- Região `SE` duplicada no legado: mantida a versão com `state = SP`.
- Código de região `XX` (active_flag=0) descartado — não representa nenhuma regional de negócio.

---

## Limitações

- **Sem SCD:** dimensões estáticas (Type 1). Mudanças históricas de segmento ou preço não são preservadas.
- **`xlsx` via pandas:** não escala para arquivos grandes.
- **Sem orquestração:** execução manual em sequência. Em produção: Databricks Workflows.
- **Sem testes formais:** validações inline nos notebooks + `tests/`. Em produção: Great Expectations ou DLT Expectations.

---

## Melhorias pendentes

- SCD Type 2 para `dim_cliente` e `dim_produto`
- Ingestão incremental com MERGE e controle de watermark
- Orquestração via Databricks Workflows
- Unity Catalog para governança e lineage
