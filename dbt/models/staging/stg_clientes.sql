select
    customer_id,
    nome_cliente,
    segmento,
    porte,
    cidade,
    estado,
    email,
    data_cadastro,
    status_cliente,
    updated_at,
    processing_timestamp
from {{ source('silver', 'silver_clientes') }}
