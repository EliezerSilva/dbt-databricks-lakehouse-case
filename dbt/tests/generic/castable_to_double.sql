{% test castable_to_double(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null and try_cast({{ column_name }} as double) is null
{% endtest %}
