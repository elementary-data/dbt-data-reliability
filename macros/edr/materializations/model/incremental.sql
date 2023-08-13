{% materialization incremental, default %}
  {% set relations = dbt.materialization_incremental_default() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="snowflake" %}
  {% set relations = dbt.materialization_incremental_snowflake() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="bigquery" %}
  {% set relations = dbt.materialization_incremental_bigquery() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="spark" %}
  {% set relations = dbt.materialization_incremental_spark() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="databricks" %}
  {% set relations = dbt.materialization_incremental_databricks() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}
