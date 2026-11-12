-- Original PySpark: dropDuplicates(["product_id"]) manteve a versao mais antiga (P0006).
-- Aqui: SCD1 - versao mais recente por updated_at.
-- Divergencia intencional declarada em seeds/reconciliacao_excecoes.csv.
with ranked as (
    select
        *,
        row_number() over (
            partition by product_id
            order by updated_at desc nulls last, nome_produto asc nulls last
        ) as rn
    from {{ ref('stg_produtos') }}
)

select
    sha2(product_id, 256) as sk_produto,
    product_id,
    nome_produto,
    categoria,
    subcategoria,
    familia,
    preco_lista,
    currency,
    tags_csv,
    status
from ranked
where rn = 1
