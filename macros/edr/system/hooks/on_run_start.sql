{% macro on_run_start() %}
  {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
  {% if not execute or edr_cli_run %}
    {{ return('') }}
  {% endif %}

  {% do elementary.warn_artifacts_autoupload_deprecation() %}

  {% do elementary.init_elementary_graph() %}

  {% if flags.WHICH in ['test', 'build'] %}
    {{ elementary.create_elementary_tests_schema() }}
  {% endif %}

  {% do elementary.create_target_dir() %}
{% endmacro %}
