{% test email_valido(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} !~* '^[^@\s]+@[^@\s]+\.[^@\s]+$'

{% endtest %}
