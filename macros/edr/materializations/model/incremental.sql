{#
  We're using `.call_macro()` here in order to avoid changing the call stack.
  We don't want to change the call stack because dbt checks the call stack is only its own materialization.
  References:
    - https://github.com/dbt-labs/dbt-core/blob/00a531d9d644e6bead6a209bc053b05ae02e48f6/core/dbt/clients/jinja.py#L328
    - https://github.com/dbt-labs/dbt-core/blob/6130a6e1d0d29b257fbcd1b17fcd730383d73ce0/core/dbt/context/providers.py#L1319
#}


{% materialization incremental, default %}
  {% set relations = dbt.materialization_incremental_default.call_macro() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="snowflake", supported_languages=["sql", "python"] %}
  {% set relations = dbt.materialization_incremental_snowflake.call_macro() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="bigquery", supported_languages=["sql", "python"] %}
  {% set relations = dbt.materialization_incremental_bigquery.call_macro() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="spark", supported_languages=["sql", "python"] %}
  {% set relations = dbt.materialization_incremental_spark.call_macro() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="databricks", supported_languages=["sql", "python"] %}
  {% set relations = dbt.materialization_incremental_databricks.call_macro() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="athena", supported_languages=["sql"] %}
  {% set relations = dbt.materialization_incremental_athena.call_macro() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization incremental, adapter="trino", supported_languages=["sql"] %}
  {% set relations = dbt.materialization_incremental_trino.call_macro() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}
