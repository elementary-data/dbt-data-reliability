{% materialization table, default %}
  {% set relations = dbt.materialization_table_default() %}
  {% set row_count = elementary.query_row_count_metric() %}
  {% do elementary.cache_row_count_metric(row_count) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization table, adapter="bigquery" %}
  {% set relations = dbt.materialization_table_bigquery() %}
  {% set row_count = elementary.query_row_count_metric() %}
  {% do elementary.cache_row_count_metric(row_count) %}
  {% do return(relations) %}
{% endmaterialization %}
