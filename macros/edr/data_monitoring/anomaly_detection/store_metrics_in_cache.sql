{% macro store_metrics_table_in_cache() %}
  {% set metrics_tables_cache = elementary.get_cache("tables").get("metrics").get("relations") %}
  {% set metrics_table = elementary.get_elementary_test_table(elementary.get_elementary_test_table_name(), 'metrics') %}
  {% if metrics_table %}
    {% do metrics_tables_cache.append(metrics_table) %}
  {% endif %}
{% endmacro %}
