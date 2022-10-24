{% macro upload_dbt_invocation() %}
  {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
  {% if not execute or edr_cli_run %}
    {{ return('') }}
  {% endif %}

  {% set relation = elementary.get_elementary_relation('dbt_invocations') %}
  {% if not relation %}
    {{ return('') }}
  {% endif %}

  {% set now_str = elementary.datetime_now_utc_as_string() %}
  {% set invocation_vars = ref.config and ref.config.vars and ref.config.vars.to_dict() %}
  {% set dbt_invocation = {
      'invocation_id': invocation_id,
      'run_started_at': elementary.run_started_at_as_string(),
      'run_completed_at': now_str,
      'generated_at': now_str,
      'command': flags.WHICH,
      'dbt_version': dbt_version,
      'full_refresh': flags.FULL_REFRESH,
      'vars': invocation_vars,
      'selected_resources': selected_resources
  } %}

  {% do elementary.insert_rows(relation, [dbt_invocation], should_commit=true) %}
{% endmacro %}

{% macro get_dbt_invocations_empty_table_query() %}
    {{ return(elementary.empty_table([
      ('invocation_id', 'long_string'),
      ('run_started_at', 'string'),
      ('run_completed_at', 'string'),
      ('generated_at', 'string'),
      ('command', 'string'),
      ('dbt_version', 'string'),
      ('full_refresh', 'string'),
      ('vars', 'long_string'),
      ('selected_resources', 'long_string'),
    ])) }}
{% endmacro %}
