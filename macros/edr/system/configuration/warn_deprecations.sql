{% macro warn_artifacts_autoupload_deprecation() %}
  {% set is_autoupload_var_defined = var("disable_dbt_artifacts_autoupload", none) %}
  {% if is_autoupload_var_defined is not none %}
      {% do return(none) %}
  {% endif %}

  {% set msg %}
In the next version, project-related artifacts (dbt_models, dbt_tests, dbt_sources, etc.) will not be automatically uploaded in the on-run-end.
If you would like to keep this behavior, please add `disable_dbt_artifacts_autoupload: false` to your vars.
Run-related artifacts (dbt_run_results, dbt_invocations, elementary_test_results, etc.) will keep working as usual.
You can remove this warning by adding `disable_dbt_artifacts_autoupload: true` to your vars.
  {% endset %}
  {% do exceptions.warn(msg) %}
{% endmacro %}
