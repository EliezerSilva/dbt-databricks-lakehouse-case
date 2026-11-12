select
    regional_code,
    regional_name as nome_regiao,
    state as estado_sede,
    manager_name as gestor,
    processing_timestamp
from {{ source('silver', 'silver_regioes') }}
