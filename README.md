# dbt + Databricks Lakehouse Case

<!-- DAB-DEV-STATUS-2026 -->

## Implantacao atual — Databricks Asset Bundles

Este projeto combina PySpark, Delta Lake, Unity Catalog,
Databricks Asset Bundles (DAB) e dbt Core.

### Estado validado em 23/10/2026

| Componente | Situacao |
|---|---|
| Databricks Asset Bundles | 14 Jobs sincronizados |
| Bronze | 4 Jobs de ingestao executados com sucesso |
| Silver — Pedidos | 400 registros validados |
| Silver — Itens | 992 registros, 992 chaves unicas |
| Silver — Cadastros e JSON | Pendentes nesta implantacao |
| Gold | Pendente de validacao nesta implantacao |
| Data Quality | 9 regras SQL verificadas |
| Observabilidade | Dashboard nativo Databricks |

### Estrutura DAB

- databricks.yml: configuracao declarativa e parametros DEV.
- resources/: definicoes dos Jobs e validacoes.
- notebooks/: ingestao, transformacoes e controles.
- tests/: testes de isolamento e configuracao.

O ambiente DEV utiliza o catalogo workspace e os schemas
bronze_dev, silver_dev, gold_dev e control_dev.

### Implantacao e controle

A validacao do Bundle utiliza os comandos:
databricks bundle validate e databricks bundle plan.

As cargas Silver iniciais possuem protecao contra reprocessamento.
A ingestao incremental e a homologacao completa da Gold
continuam como evolucoes previstas.

### Data Quality

A Silver de itens registrou:
- 3 duplicatas removidas;
- 45 divergencias financeiras corrigidas;
- 31 precos unitarios nulos identificados;
- 256 status nao informados;
- nenhuma duplicidade de chave remanescente.

[Dashboard de Operacoes e Data Quality](https://dbc-7b3b829c-38e5.cloud.databricks.com/sql/dashboardsv3/01f1cee3f8ab145f87be310388702b89?o=7474651842529377)

### Modernizacao com dbt Core

A frente dbt permanece separada da implantacao DAB.
O objetivo e modernizar transformacoes relacionais
mantendo a implementacao PySpark como referencia.

A documentacao abaixo preserva o historico e o baseline original.

---



Este repositório acompanha a evolução do projeto
[Databricks Lakehouse Case](https://github.com/EliezerSilva/databricks-lakehouse-case),
originalmente desenvolvido com PySpark, Delta Lake e arquitetura Medallion.

## Estado atual

A implementação PySpark original está preservada neste repositório como baseline de comparação.
O dbt lê as tabelas originais (`workspace.silver`, `workspace.gold`) e escreve somente em schemas isolados.

| Componente | Status |
|---|---|
| Projeto dbt Core em `dbt/` (dbt-databricks, SQL Warehouse serverless, OAuth U2M) | Implementado |
| `stg_canais` — view em `workspace.dbt_staging` | Implementado |
| `dim_canal` — table em `workspace.dbt_gold` | Implementado, testado e reconciliado com `workspace.gold.dim_canal` |
| Demais dimensões, fato e métricas | Não iniciado |

Detalhes da primeira dimensão: [docs/dbt/dim_canal.md](docs/dbt/dim_canal.md).

### Execução do dbt

O perfil fica fora do repositório, em `~/.dbt/profiles.yml`:

```yaml
lakehouse_case:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <workspace-host>
      http_path: /sql/1.0/warehouses/<warehouse-id>
      catalog: workspace
      schema: dbt
      auth_type: oauth
      threads: 1
```

```bash
cd dbt
dbt debug
dbt run
dbt test
dbt docs generate
```

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
