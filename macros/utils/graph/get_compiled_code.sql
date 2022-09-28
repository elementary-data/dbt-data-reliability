{% macro get_compiled_code(node) %}
  {% do return(node.get('compiled_code') or node.get('compiled_sql')) %}
{% endmacro %}
