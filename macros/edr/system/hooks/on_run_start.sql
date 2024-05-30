{% macro on_run_start() %}
  {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
  {% if not execute or edr_cli_run %}
    {{ return('') }}
  {% endif %}

  {% do elementary.recommend_dbt_core_artifacts_upgrade() %}
  {% do elementary.ensure_materialize_override() %}
  {% do elementary.init_elementary_graph() %}

  {% if elementary.is_test_command() %}
    {{ elementary.create_elementary_tests_schema() }}
  {% endif %}
{% endmacro %}
