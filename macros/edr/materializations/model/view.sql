{% materialization view, default %}
  {% set relations = dbt.materialization_view_default() %}
  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization view, adapter="bigquery" %}
  {% set relations = dbt.materialization_view_bigquery() %}
  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}
