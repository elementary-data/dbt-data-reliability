{% macro init_elementary_graph() %}
  {% do graph.setdefault('elementary', {
    'test_results': {}
  }) %}
{% endmacro %}
