{% macro on_run_start() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return('') %}
  {% endif %}
  
  {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
  {% if not execute or edr_cli_run %}
    {{ return('') }}
  {% endif %}

  {% do elementary.recommend_dbt_core_artifacts_upgrade() %}
  {% set runtime_config = elementary.get_elementary_runtime_config(include_defaults=false) %}
  {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
  {% set elementary_version = elementary.get_elementary_package_version() %}
  {% set runtime = {
    "config": runtime_config,
    "dbt_version": dbt_version,
    "elementary_version": elementary_version,
    "database": elementary_database,
    "schema": elementary_schema,
  } %}
  {% do elementary.edr_log("Runtime data: " ~ tojson(runtime), info=True) %}
  {% do elementary.init_elementary_graph() %}

  {% if elementary.is_test_command() %}
    {{ elementary.create_elementary_tests_schema() }}
  {% endif %}
{% endmacro %}
