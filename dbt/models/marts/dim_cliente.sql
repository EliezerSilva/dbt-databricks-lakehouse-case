-- Original PySpark: dropDuplicates(["customer_id"]) manteve a versao mais antiga (C0010, C0025).
-- Aqui: SCD1 - versao mais recente por updated_at; desempate por e-mail valido.
-- Divergencias intencionais declaradas em seeds/reconciliacao_excecoes.csv.
with ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by
                updated_at desc nulls last,
                case when email like '%@%' then 0 else 1 end,
                email asc nulls last,
                nome_cliente asc nulls last
        ) as rn
    from {{ ref('stg_clientes') }}
)

select
    sha2(customer_id, 256) as sk_cliente,
    customer_id,
    nome_cliente,
    segmento,
    porte,
    cidade,
    estado,
    email,
    data_cadastro,
    status_cliente
from ranked
where rn = 1
