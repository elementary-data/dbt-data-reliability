{% macro column_exists_in_relation(relation, column_name) %}
  {% set columns = adapter.get_columns_in_relation(relation) %}
  {% for column in columns %}
    {% if column.name.lower() == column_name.lower() %}
      {% do return(true) %}
    {% endif %}
  {% endfor %}
  {% do return(false) %}
{% endmacro %}
