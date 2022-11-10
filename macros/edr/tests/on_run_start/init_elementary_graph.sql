{% macro init_elementary_graph() %}
  {% do graph.setdefault("elementary", {
    "test_results": {},
    "tests_schema_name": none,
    "tables": {
      "metrics": [],
      "schema_snapshots": []
    }
  }) %}
{% endmacro %}
