{% macro render_run_query(prerendered_query) %}
  {% set results = elementary.run_query(render(prerendered_query)) %}
  {% set results_json = elementary.agate_to_json(results) %}
  {% do elementary.edr_log(results_json) %}
{% endmacro %}
