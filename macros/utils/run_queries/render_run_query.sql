{% macro render_run_query(prerendered_query) %}
  {% set results = elementary.run_query(render(prerendered_query)) %}
  {% do return(elementary.agate_to_dicts(results)) %}
{% endmacro %}
