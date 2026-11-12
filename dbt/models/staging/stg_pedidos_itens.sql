select
    order_id,
    item_seq,
    product_code,
    quantity,
    unit_price,
    total_item,
    item_status,
    processing_timestamp
from {{ source('silver', 'silver_pedidos_itens') }}
