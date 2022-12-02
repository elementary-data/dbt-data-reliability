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
    {% do elementary.edr_log("Uploading dbt artifacts.") %}
    {% for artifacts_model, upload_artifacts_func in model_upload_func_map.items() %}
      {% if not elementary.get_result_node(artifacts_model) %}
        {% set relation = elementary.get_elementary_relation(artifacts_model) %}
        {% if relation %}
          {% do upload_artifacts_func(should_commit=true, cache=elementary.get_config_var('cache_artifacts')) %}
        {% endif %}
      {% else %}
        {% do elementary.debug_log('[{}] Artifacts already ran.'.format(artifacts_model)) %}
      {% endif %}
    {% endfor %}
    {% do elementary.edr_log("Uploaded dbt artifacts successfully.") %}
  {% endif %}
{% endmacro %}
