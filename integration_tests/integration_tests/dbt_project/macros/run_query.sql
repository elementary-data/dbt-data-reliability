{% macro run_query(query) %}
  {% set results = elementary.run_query(render(query)) %}
  {% set results_json = elementary.agate_to_json(results) %}
  {% do elementary.edr_log(results_json) %}
{% endmacro %}
