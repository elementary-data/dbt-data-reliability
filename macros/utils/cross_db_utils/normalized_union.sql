{% macro normalized_union_prefix() %}
    union {% if target.type == 'bigquery' %} distinct {% endif %}
{% endmacro %}
