{% macro upload_dbt_artifacts() %}
  {% if execute and results %}
    {% set model_upload_func_map = {
      "dbt_models": elementary.upload_dbt_models,
      "dbt_tests": elementary.upload_dbt_tests,
      "dbt_sources": elementary.upload_dbt_sources,
      "dbt_snapshots": elementary.upload_dbt_snapshots,
      "dbt_metrics": elementary.upload_dbt_metrics,
      "dbt_exposures": elementary.upload_dbt_exposures,
      }
    %}
    {% for artifacts_model, upload_artifacts_func in model_upload_func_map.items() %}
      {% if not elementary.get_result_node(artifacts_model) %}
        {% set relation = elementary.get_elementary_relation(artifacts_model) %}
        {% do dbt.truncate_relation(relation) %}
        {% do upload_artifacts_func(should_commit=true) %}
      {% else %}
        {% do elementary.debug_log('[%s] Artifacts already ran.' % artifacts_model) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
