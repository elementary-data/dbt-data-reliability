{% macro sql_union_distinct() %}
union {% if target.type == "bigquery" %} distinct {% endif %}
{% endmacro %}
