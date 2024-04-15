{#
  We're using `.call_macro()` here in order to avoid changing the call stack.
  We don't want to change the call stack because dbt checks the call stack is only its own materialization.
  References:
    - https://github.com/dbt-labs/dbt-core/blob/00a531d9d644e6bead6a209bc053b05ae02e48f6/core/dbt/clients/jinja.py#L328
    - https://github.com/dbt-labs/dbt-core/blob/6130a6e1d0d29b257fbcd1b17fcd730383d73ce0/core/dbt/context/providers.py#L1319
#}

{% macro materialize_table(adapter_name) %}
  {% set original_name = 'materialization_table_' ~ adapter_name %}
  {% set original = dbt.get(original_name, dbt.materialization_table_default) %}
  {% set relations = original.call_macro() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmacro %}


{% materialization table, default %}
  {% do return(elementary.materialize_table.call_macro('default')) %}
{% endmaterialization %}

{% materialization table, adapter="snowflake", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialize_table.call_macro('snowflake')) %}
{% endmaterialization %}

{% materialization table, adapter="bigquery", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialize_table.call_macro('bigquery')) %}
{% endmaterialization %}

{% materialization table, adapter="spark", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialize_table.call_macro('spark')) %}
{% endmaterialization %}

{% materialization table, adapter="databricks", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialize_table.call_macro('databricks')) %}
{% endmaterialization %}

{% materialization table, adapter="redshift", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialize_table.call_macro('redshift')) %}
{% endmaterialization %}

{% materialization table, adapter="athena", supported_languages=["sql"] %}
  {% set relations = dbt.materialization_table_athena.call_macro() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(relations) %}
  {% endif %}

  {% set metrics = elementary.query_metrics() %}
  {% do elementary.cache_metrics(metrics) %}
  {% do return(relations) %}
{% endmaterialization %}

{% materialization table, adapter="trino", supported_languages=["sql"] %}
  {% do return(elementary.materialize_table.call_macro('trino')) %}
{% endmaterialization %}
