select
    delivery_id,
    order_ref,
    carrier_name,
    carrier_mode,
    delivery_status,
    dest_state,
    dest_city,
    cost,
    shipped_at,
    delivered_at,
    promised_date,
    flag_atrasado,
    dias_atraso,
    tempo_entrega_dias,
    processing_timestamp
from {{ source('silver', 'silver_logistica') }}
