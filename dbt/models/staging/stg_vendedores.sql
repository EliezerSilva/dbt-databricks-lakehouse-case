select
    seller_id,
    seller_name as nome_vendedor,
    canal_id,
    regional_code,
    hire_date as data_admissao,
    status,
    processing_timestamp
from {{ source('silver', 'silver_vendedores') }}
