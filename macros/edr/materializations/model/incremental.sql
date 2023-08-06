{% materialization incremental, default %}
  {% set relations = dbt.materialization_incremental_default() %}
  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="snowflake" %}
  {% set relations = dbt.materialization_incremental_snowflake() %}
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

{% materialization incremental, adapter="spark" %}
  {% set relations = dbt.materialization_incremental_spark() %}
  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="databricks" %}
  {% set relations = dbt.materialization_incremental_databricks() %}
  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}
