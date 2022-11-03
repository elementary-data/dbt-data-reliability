{% macro upload_dbt_invocation() %}
  {% set relation = elementary.get_elementary_relation('dbt_invocations') %}
  {% if not execute or not relation %}
    {{ return('') }}
  {% endif %}

  {% do elementary.debug_log("Uploading dbt invocation.") %}
  {% set now_str = elementary.datetime_now_utc_as_string() %}
  {% set invocation_vars = ref.config and ref.config.vars and ref.config.vars.to_dict() %}
  {% set selected_nodes = invocation_args_dict.select %}
  {% set selector = ref.config and ref.config.args and ref.config.args.selector_name %}
  {% set dbt_invocation = {
      'invocation_id': invocation_id,
      'run_started_at': elementary.run_started_at_as_string(),
      'run_completed_at': now_str,
      'generated_at': now_str,
      'command': flags.WHICH,
      'dbt_version': dbt_version,
      'elementary_version': elementary.get_elementary_package_version(),
      'full_refresh': flags.FULL_REFRESH,
      'vars': invocation_vars,
      'selected_nodes': selected_nodes,
      'selector': selector
  } %}

  {% do elementary.insert_rows(relation, [dbt_invocation], should_commit=true) %}
  {% do elementary.edr_log("Uploaded dbt invocation successfully.") %}
{% endmacro %}

{% macro get_dbt_invocations_empty_table_query() %}
    {{ return(elementary.empty_table([
      ('invocation_id', 'long_string'),
      ('run_started_at', 'string'),
      ('run_completed_at', 'string'),
      ('generated_at', 'string'),
      ('command', 'string'),
      ('dbt_version', 'string'),
      ('elementary_version', 'string'),
	  ('full_refresh', 'boolean'),
      ('vars', 'long_string'),
      ('selected_nodes', 'long_string'),
      ('selector', 'long_string')
    ])) }}
{% endmacro %}
