-- Falha se faltar qualquer dia entre a menor e a maior data da dimensao.
select count(*) as dias, datediff(max(data), min(data)) + 1 as esperado
from {{ ref('dim_data') }}
having count(*) <> datediff(max(data), min(data)) + 1
