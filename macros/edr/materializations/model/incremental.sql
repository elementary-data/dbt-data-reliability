{% materialization incremental, default %}
  {% set relations = dbt.materialization_incremental_default() %}
  {% set row_count = elementary.query_row_count_metric() %}
  {% do elementary.cache_row_count_metric(row_count) %}
  {% do return(relations) %}
{% endmaterialization %}
