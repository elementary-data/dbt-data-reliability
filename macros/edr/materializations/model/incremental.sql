{% materialization incremental, default %}
  {% set relations = dbt.materialization_incremental_default() %}
  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="bigquery" %}
  {% set relations = dbt.materialization_incremental_bigquery() %}
  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}