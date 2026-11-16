{% test reconciles_with(model, compare_model, columns, exceptions=none, key_column=none) %}
{#- EXCEPT ALL nos dois sentidos: diferencas de multiplicidade (linha duplicada de um lado) tambem falham.
    Chaves de excecao comparadas com <=> (null-safe). -#}
{%- set cols = columns | join(', ') -%}
with model_rows as (
    select {{ cols }} from {{ model }}
),

baseline_rows as (
    select {{ cols }} from {{ compare_model }}
),

diffs as (
    select 'only_in_model' as diff, * from (select * from model_rows except all select * from baseline_rows)
    union all
    select 'only_in_baseline' as diff, * from (select * from baseline_rows except all select * from model_rows)
)

{% if exceptions is none %}
select * from diffs
{% else %}
, excecoes as (
    select cast(chave as string) as chave from {{ exceptions }} where modelo = '{{ model.identifier }}'
)

select d.diff, cast(d.{{ key_column }} as string) as chave
from diffs d
where not exists (select 1 from excecoes e where e.chave <=> cast(d.{{ key_column }} as string))

union all

select 'declarada_sem_diferenca' as diff, e.chave
from excecoes e
where not exists (select 1 from diffs d where cast(d.{{ key_column }} as string) <=> e.chave)
{% endif %}
{% endtest %}
