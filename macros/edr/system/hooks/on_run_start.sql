{% macro on_run_start() %}
  {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
  {% if not execute or edr_cli_run %}
    {{ return('') }}
  {% endif %}

  {% if flags.WHICH in ['test', 'build'] %}
    {{ elementary.create_elementary_tests_schema() }}
  {% endif %}

  {% do elementary.create_target_dir() %}
{% endmacro %}
