{% macro on_run_start() %}
  {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
  {% if not execute or edr_cli_run %}
    {{ return('') }}
  {% endif %}

  {% do recommend_dbt_core_artifacts_upgrade() %}
  {% do elementary.init_elementary_graph() %}

  {% if flags.WHICH in ['test', 'build'] %}
    {{ elementary.create_elementary_tests_schema() }}
  {% endif %}
{% endmacro %}
