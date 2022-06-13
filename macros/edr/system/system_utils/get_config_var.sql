{% macro get_config_var(var_name, value=none) %}
  {% set default_config = {
    'days_back': 14,
    'anomaly_sensitivity': 3,
    'backfill_days': 2,
    'alert_dbt_model_fail': true,
    'alert_dbt_model_skip': true,
    'elementary_debug_logs': false,
    'refresh_dbt_artifacts': false,
    'dbt_artifacts_chunk_size': 50,
    'edr_cli_run': false,
    'max_int': 2147483647,
    'schemas_to_alert_on_new_tables': [],
    'custom_run_started_at': null,
    'edr_monitors': {
      'table': ['schema_changes', 'row_count', 'freshness'],
      'column_any_type': ['null_count', 'null_percent'],
      'column_string': ['min_length', 'max_length', 'average_length', 'missing_count', 'missing_percent'],
      'column_numeric': ['min', 'max', 'zero_count', 'zero_percent', 'average', 'standard_deviation', 'variance']
    } 
  } %}

  {% if value %}
    {{ return(value) }}
  {% else %}}
    {{ return(var(var_name, default_config.get(var_name))) }}
  {% endif %}
{% endmacro %}
