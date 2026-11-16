# Contrato de dados: silver (produtor) x dbt (consumidor)

## Fronteira

O dbt não lê a bronze. As 9 fontes do staging são tabelas da silver produzidas pelo pipeline PySpark original, que é
somente leitura neste projeto. O contrato fica na fronteira: o consumidor declara o que espera e falha quando o produtor
deixa de entregar.

## Camadas do contrato

| Camada | Onde | O que garante |
|---|---|---|
| Schema do produtor | `contrato_schema` nas 9 fontes (`dbt/models/staging/_sources.yml`) | Cada coluna consumida existe com o tipo declarado. Coluna nova é permitida; remoção ou troca de tipo falha. |
| Metadados | `config.meta` da fonte `silver` | Versão do contrato, produtor, consumidor, SLA declarado, política de mudança |
| Regras de conteúdo | Testes de coluna existentes (`not_null`, `unique`, `accepted_values`, `relationships`) | Chaves, domínios e integridade referencial |
| Saída do consumidor | `contract: enforced` em `fato_pedidos` e `fct_pedidos` | O dbt recusa o build se nome ou tipo de coluna da saída mudar |

O contrato cobre só as colunas que o staging consome. `metadata` de `silver_atendimento_ocorrencias` fica fora porque
nenhum modelo a lê.

## Regressão

`fixture_reconcilia_baseline` (`id bigint`, `valor string`) recebe um contrato com `valor int` e `coluna_removida`. O teste
só passa com exatamente 2 violações (`tipo_divergente` e `coluna_ausente`).

## Achados registrados pelo contrato

| Achado | Situação |
|---|---|
| `silver_logistica.cost` e `silver_produtos.list_price` são `string` | Tipo atual congelado no contrato; `custo_entrega` mantém o teste `castable_to_double` (warn). Correção depende do produtor. |
| SLA de carga | Declarado, não verificado: as fontes estão estáticas desde 2026-05 e `freshness` falharia sempre |
| Quarentena do produtor | `silver.tabelas_orfas` grava 80 itens em 160 linhas e `silver.tabelas_invalidas` grava 1 registro em 3, por append a cada execução. Tratado na fase F2. |

## Próximas fases

| Fase | Entrega |
|---|---|
| F2 | `quarentena_registros` no dbt, idempotente, reconciliada com as tabelas de quarentena da silver deduplicadas |
| F3 | Eventos append-only da quarentena (QUARENTENADO, ELEGIVEL, REPROCESSADO, REJEITADO, EXCECAO_APROVADA) |
| F4 | Reprocessamento de itens órfãos quando o cabeçalho chega, provado com seed de cabeçalho tardio no target dev |
| F5 | Agente lê a quarentena como fonte de investigação, sem permissão de liberação |
