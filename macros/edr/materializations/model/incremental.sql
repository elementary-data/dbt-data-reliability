{% materialization incremental, default %}
  {% set relations = dbt.materialization_incremental_default() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="snowflake", supported_languages=["sql", "python"] %}
  {% set relations = dbt.materialization_incremental_snowflake() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="bigquery", supported_languages=["sql", "python"] %}
  {% set relations = dbt.materialization_incremental_bigquery() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="spark", supported_languages=["sql", "python"] %}
  {% set relations = dbt.materialization_incremental_spark() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="databricks", supported_languages=["sql", "python"] %}
  {% set relations = dbt.materialization_incremental_databricks() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}
