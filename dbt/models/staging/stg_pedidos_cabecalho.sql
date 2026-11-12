select
    order_id,
    customer_code,
    seller_id,
    order_date,
    promised_date,
    status_order,
    gross_amount,
    discount_amount,
    net_amount,
    last_update,
    order_source,
    order_priority,
    processing_timestamp
from {{ source('silver', 'silver_pedidos_cabecalho') }}
