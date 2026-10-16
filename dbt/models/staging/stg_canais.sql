select
    id_canal as canal_id,
    nome_canal,
    tipo_canal,
    ativo,
    processing_timestamp
from {{ source('silver', 'silver_canais') }}
