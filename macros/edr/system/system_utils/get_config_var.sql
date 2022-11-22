{% macro get_config_var(var_name) %}

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
    'edr_cli_run': false,
    'max_int': 2147483647,
    'custom_run_started_at': null,
    'edr_monitors': {
      'table': ['schema_changes', 'row_count', 'freshness'],
      'column_any_type': ['null_count', 'null_percent'],
      'column_string': ['min_length', 'max_length', 'average_length', 'missing_count', 'missing_percent'],
      'column_numeric': ['min', 'max', 'zero_count', 'zero_percent', 'average', 'standard_deviation', 'variance']
    },
    'time_format': '%Y-%m-%d %H:%M:%S',
    'long_string_size': 16384,
    'collect_model_sql': true,
    'model_sql_max_size': 10240,
    'query_max_size': 1000000,
    'insert_rows_method': 'max_query_size'
  } %}

  {{ return(var(var_name, default_config.get(var_name))) }}
{% endmacro %}
