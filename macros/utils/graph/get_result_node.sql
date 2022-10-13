{% macro get_result_node(node_unique_id) %}
  {% for result in results %}
    {% if result.node.unique_id == node_unique_id %}
      {{ return(result.node) }}
    {% endif %}
  {% endfor %}
  {{ return(none) }}
{% endmacro %}
