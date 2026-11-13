# Agente de quarentena

Diagnostica incidentes de qualidade de dados do Lakehouse, explica cada um em linguagem de negócio
e propõe a correção. Não altera nenhuma camada de dados.

## Princípios

- Somente leitura nas camadas de dados; escrita apenas em `workspace.agent`.
- Classificação determinística; o LLM explica, não decide.
- Toda afirmação do LLM é validada contra a evidência coletada.
- Correção sempre por proposta e PR, com aprovação humana; nenhuma liberação automática de registros.

## Fontes de incidentes

| Fonte | O que detecta |
|---|---|
| `silver.tabelas_invalidas` | Registros descartados pela Silver (deduplicados pelo payload) |
| `silver.tabelas_orfas` | Itens sem cabeçalho na fato; verifica presença do cabeçalho na Bronze e na Silver e o status da carga anterior aos órfãos |
| `control.ingestion_log` | Arquivos cuja última carga ficou em NOK (drift de layout ou falha de carga) |
| Artefatos do dbt (`run_results.json`, `manifest.json`) | Testes com `fail`; para `accepted_values`, consulta os valores fora do domínio. Nós com `error` (execução) viram aviso, não incidente |

## Classificações

`DESCARTE_LEGITIMO`, `DADO_INCOMPLETO_ORIGEM`, `AUSENTE_NA_ORIGEM`, `FALHA_DE_CARGA`, `DESCARTE_A_MONTANTE`,
`ORFAO_DESATUALIZADO`, `DRIFT_DE_LAYOUT`, `DRIFT_DE_DOMINIO`, `FALHA_TESTE_DBT`, `NAO_CLASSIFICADO`.
Cada classificação tem proposta padrão (`agent/classify.py`); `DRIFT_DE_DOMINIO` gera proposta específica a partir
do registro de domínios e sinônimos (`agent/dominios.py`), embrião da ontologia.

## Garantias técnicas

| Garantia | Implementação | Teste |
|---|---|---|
| Leitura só com `SELECT`/`WITH`, um statement | `agent/db.py` `check_read` | `TestGuardas` |
| Escrita só em `workspace.agent` (`INSERT`, `CREATE TABLE IF NOT EXISTS`) | `check_write` | `TestGuardas`, `TestSqlDoAgente` |
| Schemas por lista fechada de ambientes (`original`, `dev`, `cenarios`) | `agent/tools.py` `AMBIENTES` | `test_ambiente_fora_da_lista` |
| Identificadores vindos do manifest validados por regex; valores de domínio por parâmetro | `dbt_artifacts.validar_identificadores`, `from_json(:valores)` | `test_identificadores` |
| Artefatos do dbt coerentes com o ambiente | `TARGET_POR_AMBIENTE` | `test_target` |
| Falha de uma fonte não derruba as demais (`status = PARCIAL`) | `agent/run.py` | execução |
| Resposta do LLM em JSON e números restritos à evidência | `agent/narrate.py` | `TestNumeros`, `TestNarrar` |
| Fallback para endpoint indisponível, resposta inválida e orçamento esgotado | `narrate.narrar` | `TestNarrar` |
| Ferramentas em lista fechada, parâmetros validados, limite de passos e orçamento na investigação | `agent/investigate.py` | `TestInvestigacao`, `TestValidacao` |
| Falha inesperada isolada por incidente | `investigate.investigar_todos` | `TestIsolamento` |
| Transições de estado, resolução só com fonte avaliada, supressão e reincidência | `agent/estado.py` | `TestTransicoes`, `TestAnotacao`, `TestReincidencia` |
| Lacuna de atribuição por limiar, fonte restrita ao ambiente com camada semântica, dimensão em lista fechada | `run.diagnosticos_cobertura`, `investigate.py` | `TestCobertura`, `TestFerramentaImpacto` |

## Investigação conduzida pelo LLM (tool calling)

Modo padrão (`--modo-llm investigacao`). O LLM recebe apenas o alerta (identificador, classificação, fonte, resumo e
proposta) e conduz a investigação chamando ferramentas: decide quais passos se aplicam, com quais parâmetros e quando parar.
A classificação permanece determinística e não pode ser alterada pelo LLM.

| Ferramenta | Retorno |
|---|---|
| `detalhar_incidente` | Contagens, impacto, proposta e ponteiros (arquivos relacionados, domínio afetado) |
| `amostrar_registros` | Até 5 registros afetados (consulta real à quarentena para órfãos; para lacunas, registros sem correspondência na cadeia de atribuição da [ontologia](../ontologia.md)) |
| `historico_cargas` | Últimas 5 cargas de um arquivo no `ingestion_log` |
| `dominio_valores` | Códigos válidos e sinônimos do domínio |
| `impacto_metricas` | Receita líquida e pedidos por valor de uma dimensão, via `MEASURE()` (somente ambiente `original`) |

Controles: lista fechada de ferramentas; parâmetros validados (extras rejeitados, `incidente_id` restrito à execução,
limite de 1 a 5, nome de arquivo por regex, domínio por enumeração); chamada rejeitada volta ao LLM como erro;
no máximo 4 chamadas por incidente, depois a resposta é forçada sem ferramentas; orçamento global de chamadas;
números da resposta restritos às saídas das ferramentas efetivamente chamadas; falha inesperada isolada por incidente.
Rastro de cada chamada em `workspace.agent.chamadas_ferramentas`.

Execução de referência em 2026-11-12 (ambiente `cenarios`): 4 incidentes, 12 chamadas de ferramenta, todas válidas
(4 `detalhar_incidente`, 4 `amostrar_registros`, 3 `historico_cargas`, 1 `dominio_valores`); `historico_cargas` apenas
nos incidentes com arquivo relacionado e `dominio_valores` apenas no incidente de domínio; nomes de arquivo obtidos dos
ponteiros do passo anterior; 16 chamadas ao LLM, 31.212 tokens de entrada; 4/4 narrativas aprovadas na validação.

O modo `--modo-llm narrativa` mantém o comportamento anterior (contexto completo, uma chamada por incidente).

## Cobertura da camada semântica

Fonte `cobertura_semantica` (somente ambiente `original`, cuja Silver alimenta a `dbt_gold`): mede via `MEASURE()`
na `mv_vendas` a parcela da receita líquida sem atribuição em canal, região e segmento. Acima de 1% vira incidente
`LACUNA_DE_ATRIBUICAO`, com o valor sem atribuição como impacto. O incidente informa o impacto, não afirma a causa;
a causa é investigada pelo LLM com as ferramentas. Em `dev` e `cenarios` a fonte não é avaliada (sem falha e sem
resolução de incidentes).

Execução de referência em 2026-11-13 (ambiente `original`):

| Dimensão | Receita sem atribuição | Parcela | Pedidos |
|---|---|---|---|
| Canal | R$ 527.929,23 | 15,6% | 39 |
| Região | R$ 153.769,72 | 4,5% | 19 |
| Segmento | R$ 684.460,10 | 20,2% | 61 |

São dados que passaram por quarentena, cargas e testes dbt, sem nenhum incidente nessas fontes; só a camada
semântica revela que não podem ser atribuídos.

## Controle de estado

O ciclo de vida de cada incidente é um log de eventos append-only em `workspace.agent.incidentes_eventos`;
o estado atual é o último evento. Mantém a guarda de escrita (somente `INSERT`) e o histórico completo.

| Situação na execução | Evento | Estado |
|---|---|---|
| Incidente inédito | `NOVO` | `ABERTO` |
| Incidente aberto detectado de novo | `DETECTADO` | `ABERTO` |
| Incidente reconhecido detectado de novo | `DETECTADO` (nota do reconhecimento propagada) | `RECONHECIDO` |
| Incidente resolvido detectado de novo | `REABERTO` | `ABERTO` |
| Incidente aberto ou reconhecido não detectado, com a fonte dele avaliada | `RESOLVIDO` | `RESOLVIDO` |
| Reconhecimento humano (`python -m agent.estado reconhecer`) | `RECONHECIDO` (ator humano, com nota) | `RECONHECIDO` |

O estado altera o comportamento: incidente reconhecido não é reinvestigado (sem chamada ao LLM; narrativa
`SUPRIMIDO_RECONHECIDO` com a nota); incidente reaberto chega ao LLM marcado como reincidente e a narrativa recebe,
de forma determinística, a indicação de reincidência. Resolução só ocorre quando a fonte do incidente foi avaliada
na execução (evita "resolver" falhas de dbt quando os artefatos não foram lidos); sem estado confiável, nenhum evento é gerado.

Execução de referência em 2026-11-12 (ambiente `cenarios`):

| Execução | Ação | Resultado |
|---|---|---|
| R1 | Primeira execução | 4 incidentes `NOVO` |
| — | Reconhecimento humano de G1 | `RECONHECIDO` com nota |
| R2 | Execução com LLM | G1 suprimido; 12 chamadas ao LLM (16 com os 4 incidentes) |
| R3 | Carga de `vendedores.csv` corrigida | G4 `RESOLVIDO` |
| R4 | Layout divergente volta; execução com LLM | G4 `REABERTO` (1 reabertura, 3 detecções) |

## Execução agendada

O agente roda como job serverless do Databricks, definido no bundle operacional `ops/agente/` (separado do bundle
de desenvolvimento, cujo modo pausa agendamentos a cada deploy). O bundle sincroniza apenas `agent/`, `jobs/` e
`ontologia/`.

| Item | Valor |
|---|---|
| Job | `agente-quarentena` |
| Agenda | Segunda a sexta, 07:00 (America/Sao_Paulo) |
| Parâmetros | Ambiente `original`, com gravação e investigação por LLM |
| Autenticação | Nativa do runtime (sem perfil de CLI); executa com a identidade do dono do job |
| Custo por execução | Cerca de 17 chamadas ao LLM e consultas ao SQL Warehouse; cai à medida que incidentes são reconhecidos |
| Pausar | `pause_status: PAUSED` em `ops/agente/resources/agente_quarentena.yml` e `databricks bundle deploy -t ops` em `ops/agente` |

Primeira execução pelo bundle operacional em 2026-11-13: `TERMINATED SUCCESS`, 5 incidentes atualizados no estado.

Limitação do Free Edition: service principal com segredo OAuth depende de recursos de conta indisponíveis na
edição; por isso o CI do GitHub não executa contra o Databricks, e a execução de integração (dbt build e agente)
é feita localmente.

## Persistência

| Tabela | Conteúdo |
|---|---|
| `workspace.agent.execucoes` | Uma linha por execução: ambiente, modo, status, contagem de incidentes, chamadas SQL e LLM, avisos |
| `workspace.agent.diagnosticos` | Um registro por incidente: classificação, ocorrências, impacto, decisão humana, resumo, proposta e evidência (JSON) |
| `workspace.agent.narrativas` | Explicação do LLM por incidente, status de validação, modelo e tokens |
| `workspace.agent.chamadas_ferramentas` | Rastro das chamadas de ferramenta: passo, ferramenta, parâmetros, status, linhas e duração |
| `workspace.agent.incidentes_eventos` | Ciclo de vida dos incidentes (append-only): evento, estado resultante, ator e nota |

## Execução

    python -m agent.run --ambiente original --dbt-target-dir dbt/target
    python -m agent.run --ambiente cenarios --dbt-target-dir dbt/target --gravar
    python -m agent.run --sem-llm
    python -m agent.run --simular-falha INDISPONIVEL

Sem `--gravar`, apenas imprime. Testes: `python -m unittest discover -s tests/agent`.

## Benchmark — 2026-11-12

Cenários em `tests/cenarios/`. Execução de referência `7a7aeabb473c4d77a51280fcdfa8b5e8` (ambiente `cenarios`).

| Cenário | Origem | Resultado |
|---|---|---|
| G1 — descarte legítimo | real | `DESCARTE_LEGITIMO` (região XX inativa) |
| G2 — drift de domínio | simulado | `DRIFT_DE_DOMINIO`, `SUL.` fora do domínio, proposta de sinônimo de `S` |
| G3 — órfãos | real | `AUSENTE_NA_ORIGEM`, 49 pedidos, 80 itens, R$ 434.200,62 fora da Gold |
| G4 — drift de layout | simulado | `DRIFT_DE_LAYOUT`, 5 colunas contra 6 |
| G5 — duplicação por reexecução | real | 1 incidente com 3 ocorrências |

Resultado: 5/5. Narrativas: 4/4 aprovadas na validação contra a evidência.

## Limitações

- G2 e G4 são simulados por SQL em `*_cen`: as guardas de isolamento e de replay do pipeline DAB bloqueiam,
  por desenho, o reprocessamento de fontes alteradas (comportamento confirmado em 2026-11-12).
- A quarentena não tem identificador de execução; a deduplicação usa o payload sem `processing_timestamp`.
- Regras de classificação de inválidos existem apenas para `silver_regioes`; demais motivos caem em `NAO_CLASSIFICADO`.
- Execução local (Mac + SDK). Empacotamento como job do bundle está no backlog.

## Backlog identificado durante a implementação

| Item | Origem |
|---|---|
| Janela de deduplicação de regiões na Silver não é determinística para `S` (duas linhas sem `state = SP`) | Leitura da fonte `legado_regioes_pipe.txt` |
| `validate_csv_layout` não detecta coluna renomeada (só delimitador e contagem) | Leitura de `00_setup.py` |
| Job `enterprise-lakehouse-pipeline`, fora do bundle, usa os schemas originais | Histórico Delta da Bronze original |
| Agente como job do bundle e marcação de execuções de validação | Este trabalho |
| Limpeza de `*_cen`, `dbt_cen_*` e do volume `workspace.cenarios.fontes` | Este trabalho (requer aprovação) |
