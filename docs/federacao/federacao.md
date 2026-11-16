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
| Criação e rotação de senha da conexão | `scripts/federacao_conexao.sh` (lê o `.env` do provedor, testa a credencial antes e recria conexão e catálogo) |
| Fonte dbt `legado` | `dbt/models/staging/_sources.yml`, com `contrato_schema` (varchar(10), char(7), decimal(14,2)) e unicidade por canal e mês |
| Consumo | `stg_metas_comerciais` e `meta_vs_realizado` (meta x receita líquida da `fato_pedidos`) |

O `contrato_schema` lê os tipos pelo adaptador (DESCRIBE), e não pelo `information_schema`: no catálogo estrangeiro o
`information_schema` é o do PostgreSQL, sem `full_data_type`.

## Governança

- O consumidor recebe `USE CATALOG`, `USE SCHEMA` e `SELECT` no catálogo estrangeiro; a conexão (credencial) fica com o
  dono da plataforma.
- A senha nunca é versionada nem digitada no terminal.

## Rotação de senha

1. No provedor, gerar a nova senha do usuário e baixar o `.env` (precisa de `PGUSER` e `PGPASSWORD`).
2. `source scripts/federacao_conexao.sh && federacao_conexao ~/Downloads/env.txt`
3. O script testa a credencial a partir da máquina local. Se o PostgreSQL recusar, para sem alterar nada no Databricks.
4. Com a credencial válida, recria o catálogo estrangeiro e a conexão e consulta a tabela federada.
5. Apagar o `.env` baixado.

A conexão é recriada, e não atualizada: em 2026-11-16, `databricks connections update` respondeu sucesso sem aplicar a
nova senha, e a consulta federada continuou recusada até a recriação. O catálogo estrangeiro guarda só metadados, então
recriar não perde dado. Dependência local do script: `psycopg[binary]` (não entra no CI nem no job).

## Dependência externa

Os modelos e testes federados têm a tag `federacao`. Se o banco externo estiver indisponível, o restante do projeto roda
com `dbt build --exclude tag:federacao`. O CI não conecta em nenhum banco (só `dbt parse`).

## Evidência

- Consulta federada pelo SQL Warehouse: 10 metas, total 1.030.000,00, sem cópia para o lakehouse.
- Saída de rede do Free Edition para o host externo: permitida (a consulta passou).
