{% macro arithmetic_max(val1, val2) %}
   (0.5 * ({{ val1 }} + {{ val2 }} + abs({{ val1 }} - {{ val2 }})))
{% endmacro %}

{% macro arithmetic_min(val1, val2) %}
   (0.5 * ({{ val1 }} + {{ val2 }} - abs({{ val1 }} - {{ val2 }})))
{% endmacro %}
