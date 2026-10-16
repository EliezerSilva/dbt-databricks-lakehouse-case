with dbt_model as (
    select sk_canal, canal_id, nome_canal, tipo_canal, ativo
    from {{ ref('dim_canal') }}
),

baseline as (
    select sk_canal, canal_id, nome_canal, tipo_canal, ativo
    from {{ source('pyspark_gold', 'dim_canal') }}
)

select 'only_in_dbt' as diff, * from (select * from dbt_model except select * from baseline)
union all
select 'only_in_pyspark' as diff, * from (select * from baseline except select * from dbt_model)
