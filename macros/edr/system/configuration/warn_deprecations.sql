{% macro warn_artifacts_autoupload_deprecation() %}
  {% set is_autoupload_var_defined = var("disable_dbt_artifacts_autoupload", none) %}
  {% if is_autoupload_var_defined is not none %}
      {% do return(none) %}
  {% endif %}

  {% set msg %}
To improve on-run-end hook's performance, from next version Elementary's CLI (edr) will automatically upload project-related artifacts (dbt_models, dbt_tests, dbt_sources, etc.) instead of uploading during the on-run-end hook.
If you wish to keep it running in your on-run-end hook, add `disable_dbt_artifacts_autoupload: false` to your vars.
Otherwise, you can mute this warning by adding `disable_dbt_artifacts_autoupload: true` to your vars.
The on-run-end hook will still upload run-related artifacts (dbt_run_results, dbt_invocations, elementary_test_results, etc.).
  {% endset %}
  {% do exceptions.warn(msg) %}
{% endmacro %}
