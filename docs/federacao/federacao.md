# Lakehouse Federation: sistema legado de metas

## O que demonstra

Um banco PostgreSQL externo ao Databricks (o "sistema legado" de metas comerciais) é consultado pelo Unity Catalog sem
cópia de dados. O dbt consome a tabela federada como qualquer outra fonte, sob o mesmo contrato de dados da silver.

```
PostgreSQL externo  --conexao legado_pg-->  catalogo estrangeiro legado_pg  -->  dbt source legado
public.metas_comerciais                     legado_pg.public.metas_comerciais     stg_metas_comerciais -> meta_vs_realizado
```

## Peças

| Peça | Onde |
|---|---|
| Conexão `legado_pg` (tipo POSTGRESQL) | Unity Catalog; host, usuário e senha guardados na conexão |
| Catálogo estrangeiro `legado_pg` | `ops/federacao/legado_pg.sql` |
| Criação e rotação de senha da conexão | `scripts/federacao_conexao.sh` (lê a string da área de transferência, não do terminal) |
| Fonte dbt `legado` | `dbt/models/staging/_sources.yml`, com `contrato_schema` (varchar(10), char(7), decimal(14,2)) e unicidade por canal e mês |
| Consumo | `stg_metas_comerciais` e `meta_vs_realizado` (meta x receita líquida da `fato_pedidos`) |

O `contrato_schema` lê os tipos pelo adaptador (DESCRIBE), e não pelo `information_schema`: no catálogo estrangeiro o
`information_schema` é o do PostgreSQL, sem `full_data_type`.

## Governança

- O consumidor recebe `USE CATALOG`, `USE SCHEMA` e `SELECT` no catálogo estrangeiro; a conexão (credencial) fica com o
  dono da plataforma.
- A senha nunca é versionada nem digitada no terminal. Rotação: trocar a senha no provedor, copiar a nova string e rodar
  `source scripts/federacao_conexao.sh && federacao_conexao` (faz `update` da conexão existente).

## Dependência externa

Os modelos e testes federados têm a tag `federacao`. Se o banco externo estiver indisponível, o restante do projeto roda
com `dbt build --exclude tag:federacao`. O CI não conecta em nenhum banco (só `dbt parse`).

## Evidência

- Consulta federada pelo SQL Warehouse: 10 metas, total 1.030.000,00, sem cópia para o lakehouse.
- Saída de rede do Free Edition para o host externo: permitida (a consulta passou).
