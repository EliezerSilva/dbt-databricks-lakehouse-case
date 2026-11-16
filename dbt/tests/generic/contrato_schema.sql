{% test contrato_schema(model, colunas) %}
{#- Contrato produtor x consumidor: cada coluna declarada deve existir no produtor com o tipo declarado.
    Colunas extras no produtor sao permitidas (mudanca aditiva). Uma linha por violacao.
    Tipos lidos pelo adaptador (DESCRIBE), o que funciona tambem para catalogos federados. -#}
{%- set sinonimos = {'long': 'bigint', 'integer': 'int', 'short': 'smallint', 'byte': 'tinyint', 'timestamp_ltz': 'timestamp'} -%}
{%- set atual = {} -%}
{%- if execute -%}
    {%- for c in adapter.get_columns_in_relation(model) -%}
        {%- set t = c.dtype | lower | replace(' ', '') -%}
        {%- do atual.update({c.name | lower: sinonimos.get(t, t)}) -%}
    {%- endfor -%}
{%- endif -%}
{%- set violacoes = [] -%}
{%- for nome, tipo in colunas.items() -%}
    {%- set n = nome | lower -%}
    {%- set t = tipo | lower | replace(' ', '') -%}
    {%- if n not in atual -%}
        {%- do violacoes.append(['coluna_ausente', n, t, none]) -%}
    {%- elif atual[n] != t -%}
        {%- do violacoes.append(['tipo_divergente', n, t, atual[n]]) -%}
    {%- endif -%}
{%- endfor -%}
{%- if not execute or violacoes | length == 0 %}
select cast(null as string) as violacao, cast(null as string) as coluna, cast(null as string) as tipo_contrato, cast(null as string) as tipo_produtor
where 1 = 0
{%- else %}
{%- for v in violacoes %}
select '{{ v[0] }}' as violacao, '{{ v[1] }}' as coluna, '{{ v[2] }}' as tipo_contrato, {{ "'" ~ v[3] ~ "'" if v[3] else "cast(null as string)" }} as tipo_produtor
{{ "union all" if not loop.last }}
{%- endfor %}
{%- endif %}
{% endtest %}
