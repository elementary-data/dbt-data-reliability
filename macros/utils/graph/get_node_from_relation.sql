{% macro get_node_from_relation_str(relation) %}
  {% set model_nodes = graph.nodes.values() | selectattr('resource_type', '==', 'model') %}
  {% for model_node in model_nodes %}
    {% if relation.upper() == elementary.model_node_to_full_name(model_node) %}
      {% do return(model_node) %}
    {% endif %}
  {% endfor %}
  {% do return(none) %}
{% endmacro %}
