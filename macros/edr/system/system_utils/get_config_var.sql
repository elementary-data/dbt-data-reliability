{% macro get_config_var(var_name) %}
  {% set default_config = elementary.get_default_config() %}
  {% set var_value = var(var_name, default_config.get(var_name)) %}
  {% if var_value is string %}
    {% if var_value.lower() == "true" %}
      {% do return(true) %}
    {% elif var_value.lower() == "false" %}
      {% do return(false) %}
    {% endif %}
  {% endif %}
  {% do return(var_value) %}
{% endmacro %}

{% macro get_default_config(var_name) %}
    {{ return(adapter.dispatch('get_default_config', 'elementary')()) }}
{%- endmacro -%}

{%- macro default__get_default_config() -%}
{# We use this macro to define and call vars, as the global vars defined in dbt_project.yml
   of the package are not accesible at on-run-start and on-run-end #}
  {% set default_config = {
    'days_back': 14,
    'anomaly_sensitivity': 3,
    'backfill_days': 2,
    'tests_schema_name': '',
    'debug_logs': false,
    'disable_warn_alerts': false,
    'disable_model_alerts': false,
    'disable_test_alerts': false,
    'disable_source_freshness_alerts': false,
    'disable_run_results': false,
    'disable_tests_results': false,
    'disable_dbt_artifacts_autoupload': false,
    'disable_dbt_invocation_autoupload': false,
    'disable_skipped_model_alerts': true,
    'disable_skipped_test_alerts': true,
    'dbt_artifacts_chunk_size': 5000,
    'test_sample_row_count': 5,
    'edr_cli_run': false,
    'max_int': 2147483647,
    'custom_run_started_at': none,
    'edr_monitors': elementary.get_default_monitors(),
    'long_string_size': 65535,
    'collect_model_sql': true,
    'model_sql_max_size': 10240,
    'query_max_size': 1000000,
    'insert_rows_method': 'max_query_size',
    'upload_artifacts_method': 'diff',
    'project_name': none,
    'elementary_full_refresh': false,
    'min_training_set_size': 14,
    'cache_artifacts': true,
    'anomaly_direction': 'both',
    'store_result_rows_in_own_table': true
  } %}
  {{- return(default_config) -}}
{%- endmacro -%}

{%- macro bigquery__get_default_config() -%}
    {% set default_config = elementary.default__get_default_config() %}
    {% do default_config.update({'query_max_size': 250000}) %}
    {{- return(default_config) -}}
{%- endmacro -%}
