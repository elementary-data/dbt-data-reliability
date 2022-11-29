{% macro upload_dbt_invocation() %}
  {% set relation = elementary.get_elementary_relation('dbt_invocations') %}
  {% if not execute or not relation %}
    {{ return('') }}
  {% endif %}

  {% do elementary.edr_log("Uploading dbt invocation.") %}
  {% set now_str = elementary.datetime_now_utc_as_string() %}
  {% set dbt_invocation = {
      'invocation_id': invocation_id,
      'run_started_at': elementary.run_started_at_as_string(),
      'run_completed_at': now_str,
      'generated_at': now_str,
      'command': flags.WHICH,
      'dbt_version': dbt_version,
      'elementary_version': elementary.get_elementary_package_version(),
      'full_refresh': flags.FULL_REFRESH,
      'invocation_vars': elementary.get_invocation_vars(),
      'vars': elementary.get_all_vars(),
      'target_name': target.name,
      'target_database': elementary.target_database(),
      'target_schema': target.schema,
      'target_profile_name': target.profile_name,
      'threads': target.threads,
      'selected': elementary.get_invocation_select_filter(),
      'yaml_selector': elementary.get_invocation_yaml_selector()
  } %}

  {% do elementary.insert_rows(relation, [dbt_invocation], should_commit=true) %}
  {% do elementary.edr_log("Uploaded dbt invocation successfully.") %}
{% endmacro %}

{%- macro get_invocation_select_filter() -%}
    {% set config = elementary.get_runtime_config() %}
    {%- if invocation_args_dict and invocation_args_dict.select -%}
        {{- return(invocation_args_dict.select) -}}
    {%- elif config.args and config.args.select -%}
        {{- return(config.args.select) -}}
    {%- else -%}
        {{- return([]) -}}
    {%- endif -%})
{%- endmacro -%}

{%- macro get_invocation_yaml_selector() -%}
    {% set config = elementary.get_runtime_config() %}
    {%- if invocation_args_dict and invocation_args_dict.selector_name -%}
        {{- return(invocation_args_dict.selector_name) -}}
    {%- elif config.args and config.args.selector_name -%}
        {{- return(config.args.selector_name) -}}
    {%- else -%}
        {{- return([]) -}}
    {%- endif -%})
{%- endmacro -%}

{%- macro get_invocation_vars() -%}
    {% set config = elementary.get_runtime_config() %}
    {%- if invocation_args_dict and invocation_args_dict.vars -%}
        {{- return(fromyaml(invocation_args_dict.vars)) -}}
    {%- elif config.cli_vars -%}
        {{- return(config.cli_vars) -}}
    {%- else -%}
        {{- return({}) -}}
    {%- endif -%}
{%- endmacro -%}

{%- macro get_all_vars() -%}
    {% set all_vars = {} %}
    {% set config = elementary.get_runtime_config() %}
    {%- if config.vars -%}
        {% do all_vars.update(config.vars.to_dict()) %}
    {%- endif -%}
    {% do all_vars.update(elementary.get_invocation_vars()) %}
    {{- return(all_vars) -}}
{%- endmacro -%}

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
      ('invocation_vars', 'long_string'),
      ('vars', 'long_string'),
      ('target_name', 'string'),
      ('target_database', 'string'),
      ('target_schema', 'string'),
      ('target_profile_name', 'string'),
      ('threads', 'int'),
      ('selected', 'long_string'),
      ('yaml_selector', 'long_string')
    ])) }}
{% endmacro %}
