select
    product_id,
    product_name as nome_produto,
    category as categoria,
    subcategory as subcategoria,
    family as familia,
    list_price as preco_lista,
    currency,
    tags_csv,
    status,
    updated_at,
    processing_timestamp
from {{ source('silver', 'silver_produtos') }}
