{% test reconciles_with(model, compare_model, columns, exceptions=none, key_column=none) %}
{%- set cols = columns | join(', ') -%}
with model_rows as (
    select {{ cols }} from {{ model }}
),

baseline_rows as (
    select {{ cols }} from {{ compare_model }}
),

diffs as (
    select 'only_in_model' as diff, * from (select * from model_rows except select * from baseline_rows)
    union all
    select 'only_in_baseline' as diff, * from (select * from baseline_rows except select * from model_rows)
)

{% if exceptions is none %}
select * from diffs
{% else %}
, excecoes as (
    select chave from {{ exceptions }} where modelo = '{{ model.identifier }}'
)

select diff, cast({{ key_column }} as string) as chave
from diffs
where cast({{ key_column }} as string) not in (select chave from excecoes)

union all

select 'declarada_sem_diferenca' as diff, chave
from excecoes
where chave not in (select cast({{ key_column }} as string) from diffs)
{% endif %}
{% endtest %}
