-- Original PySpark: dropDuplicates(["regional_code"]) sobre uma Silver ja unica por regional_code.
-- Aqui: sem deduplicacao; unicidade garantida por teste na source (falha em vez de escolher linha).
select
    sha2(regional_code, 256) as sk_regiao,
    regional_code,
    nome_regiao,
    estado_sede,
    gestor
from {{ ref('stg_regioes') }}
