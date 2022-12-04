{% macro on_run_end() %}
  {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
  {% if not execute or edr_cli_run %}
    {{ return('') }}
  {% endif %}

  {% if not elementary.get_config_var('disable_dbt_artifacts_autoupload') %}
    {{ elementary.upload_dbt_artifacts() }}
  {% endif %}

  {% if not elementary.get_config_var('disable_run_results') %}
    {{ elementary.upload_run_results() }}
  {% endif %}

  {% if flags.WHICH in ['test', 'build'] and not elementary.get_config_var('disable_tests_results') %}
    {{ elementary.handle_tests_results() }}
  {% endif %}

  {% if not elementary.get_config_var('disable_dbt_invocation_autoupload') %}
    {{ elementary.upload_dbt_invocation() }}
  {% endif %}
{% endmacro %}
