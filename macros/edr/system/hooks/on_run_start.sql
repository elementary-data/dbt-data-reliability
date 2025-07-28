{% macro on_run_start() %}
  {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
  {% if not execute or edr_cli_run %}
    {{ return('') }}
  {% endif %}

  {% do elementary.recommend_dbt_core_artifacts_upgrade() %}
  {% do elementary.ensure_materialize_override() %}
  {% set runtime_config = elementary.get_elementary_runtime_config() %}
  {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
  {% set runtime = {
    "config": runtime_config,
    "dbt_version": dbt_version,
    "database": elementary_database,
    "schema": elementary_schema,
  } %}
  {% do log("Elementary runtime: " ~ tojson(runtime), info=True) %}
  {% do elementary.init_elementary_graph() %}

  {% if elementary.is_test_command() %}
    {{ elementary.create_elementary_tests_schema() }}
  {% endif %}
{% endmacro %}
