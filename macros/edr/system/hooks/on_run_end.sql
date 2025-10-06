{% macro on_run_end() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return('') %}
  {% endif %}

  {%- if execute and not elementary.is_docs_command() %}
      {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
      {% if not execute or edr_cli_run %}
        {% do return("") %}
      {% endif %}

      {% if not elementary.get_config_var('disable_dbt_artifacts_autoupload') %}
        {% do elementary.upload_dbt_artifacts() %}
      {% endif %}

      {% if not elementary.get_config_var('disable_run_results') %}
        {% do elementary.upload_run_results() %}
      {% endif %}

      {% if elementary.is_test_command() and not elementary.get_config_var('disable_tests_results') %}
        {% do elementary.handle_tests_results() %}
      {% endif %}

      {% if elementary.is_freshness_command() and not elementary.get_config_var('disable_freshness_results') %}
        {% do elementary.upload_source_freshness() %}
      {% endif %}

      {% if not elementary.get_config_var('disable_dbt_invocation_autoupload') %}
        {% do elementary.upload_dbt_invocation() %}
      {% endif %}

      {# 
        Elementary temp tables are not really temp and should be cleaned on the end of the run.
        We want to make sure we clean the temp tables even if elementary on run end hooks are disabled.

        IMPORTANT! - This must be running last because other hooks are relaying on the temp tables.
      #}
      {% if elementary.get_config_var("clean_elementary_temp_tables") %}
        {% do elementary.clean_elementary_temp_tables() %}
      {% endif %}
  {% endif %}
{% endmacro %}
