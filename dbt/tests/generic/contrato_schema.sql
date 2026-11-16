{% test contrato_schema(model, colunas) %}
{#- Contrato produtor x consumidor: cada coluna declarada deve existir no produtor com o tipo declarado.
    Colunas extras no produtor sao permitidas (mudanca aditiva). Uma linha por violacao. -#}
with esperado as (
    select * from values
    {%- for nome, tipo in colunas.items() %}
        ('{{ nome | lower }}', '{{ tipo | lower }}'){{ "," if not loop.last }}
    {%- endfor %}
    as t(coluna, tipo)
),

atual as (
    select lower(column_name) as coluna, lower(full_data_type) as tipo
    from {{ model.database }}.information_schema.columns
    where table_schema = '{{ model.schema }}'
      and table_name = '{{ model.identifier }}'
)

select 'coluna_ausente' as violacao, e.coluna, e.tipo as tipo_contrato, cast(null as string) as tipo_produtor
from esperado e
where not exists (select 1 from atual a where a.coluna = e.coluna)

union all

select 'tipo_divergente' as violacao, e.coluna, e.tipo as tipo_contrato, a.tipo as tipo_produtor
from esperado e
join atual a on a.coluna = e.coluna
where a.tipo <> e.tipo
{% endtest %}
