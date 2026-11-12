select
    ticket_id,
    order_id,
    customer_code,
    event_type,
    severity,
    status,
    created_at,
    processing_timestamp
from {{ source('silver', 'silver_atendimento_ocorrencias') }}
