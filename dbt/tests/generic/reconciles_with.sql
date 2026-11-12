{% test reconciles_with(model, compare_model, columns) %}
{%- set cols = columns | join(', ') -%}
with model_rows as (
    select {{ cols }} from {{ model }}
),

baseline_rows as (
    select {{ cols }} from {{ compare_model }}
)

select 'only_in_model' as diff, * from (select * from model_rows except select * from baseline_rows)
union all
select 'only_in_baseline' as diff, * from (select * from baseline_rows except select * from model_rows)
{% endtest %}
