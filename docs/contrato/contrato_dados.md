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

## Quarentena do consumidor (F2)

`dbt_quarentena.quarentena_registros` é uma view com uma linha por (entidade, regra, chave):

| Origem | Regra | Registros |
|---|---|---|
| `consumidor_dbt` | `item_sem_cabecalho`: item cujo `order_id` não existe no cabeçalho | 80 itens de 49 pedidos |
| `produtor_pyspark` | Motivo gravado pelo PySpark em `silver.tabelas_invalidas`, deduplicado | 1 região (`XX`) |

Garantias: `unique_combination` em (entidade, regra, chave) e reconciliação dos itens órfãos com `silver.tabelas_orfas`
deduplicada (`assert_quarentena_reconcilia_orfaos`). Por ser derivada do estado atual, a view é idempotente: reexecutar
não duplica, e um item sai da quarentena quando o cabeçalho dele aparece.

## Ciclo de vida da quarentena (F3)

Eventos append-only em `workspace.dbt_quarentena.quarentena_eventos`; o estado atual de cada (entidade, regra, chave) é o
último evento. A escrita passa pela mesma guarda do agente (`agent/db.py`), restrita a essa tabela.

| Evento | Quem gera | Quando |
|---|---|---|
| QUARENTENADO | sincronização | registro aparece em `quarentena_registros` pela primeira vez |
| ELEGIVEL | sincronização | registro saiu da quarentena (a regra deixou de ser violada) |
| REPROCESSADO | sincronização | registro elegível presente na tabela de destino (`fato_pedidos`, `dim_regiao`) |
| REINCIDENTE | sincronização | registro elegível ou reprocessado volta a violar a regra (status QUARENTENADO) |
| REJEITADO | humano, com ator e nota | descarte definitivo, a partir de QUARENTENADO ou ELEGIVEL |
| EXCECAO_APROVADA | humano, com ator e nota | aceito fora do destino, a partir de QUARENTENADO |

REJEITADO e EXCECAO_APROVADA são terminais: a sincronização não os altera. Nenhum evento libera um item órfão para a fato
sem cabeçalho; a liberação acontece porque o dado de origem chegou, não por aprovação.

```bash
python -m quarentena.ciclo sincronizar
python -m quarentena.ciclo listar
python -m quarentena.ciclo rejeitar --entidade regioes --regra "active_flag=0 ou regional_name nulo" --chave XX --ator eli --nota "..."
```

## Reprocessamento de órfãos (F4)

Um item órfão só sai da quarentena quando o cabeçalho dele chega; nenhuma aprovação o coloca na fato. Como as fontes
estão estáticas, a chegada é simulada em schemas isolados (`dbt_sim_staging`, `dbt_sim_gold`, `dbt_sim_quarentena`):

| Peça | Papel |
|---|---|
| `seeds/cabecalhos_tardios.csv` | Cabeçalho sintético do pedido O00021 (3 itens em quarentena) |
| `stg_pedidos_cabecalho` | Inclui o seed só com `simular_cabecalho_tardio`; o build falha se a variável for usada no schema `dbt` |
| `scripts/simular_cabecalho_tardio.sh` | `sim_dbt`: roda o dbt com um profile temporário que só troca o schema para `dbt_sim` |
| `python -m quarentena.ciclo --prefixo dbt_sim` | Ciclo de eventos na simulação; a guarda de escrita aceita só `dbt_sim_quarentena.quarentena_eventos` além da tabela real |

Sequência provada: sincronização sem cabeçalho (81 QUARENTENADO) → build com o cabeçalho tardio (os itens do O00021 entram
na `fato_pedidos` e saem da quarentena) → sincronização (ELEGIVEL e REPROCESSADO para os 3 itens) → nova sincronização sem
eventos → novo build sem duplicar a fato. As reconciliações com o baseline ficam fora da simulação, onde a fato tem itens a
mais de propósito.

## Próximas fases

| Fase | Entrega |
|---|---|
| F5 | Agente lê a quarentena como fonte de investigação, sem permissão de liberação |
