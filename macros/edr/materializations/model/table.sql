{% materialization table, default %}
  {% set relations = dbt.materialization_table_default() %}
  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization table, adapter="bigquery" %}
  {% set relations = dbt.materialization_table_bigquery() %}
  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}
