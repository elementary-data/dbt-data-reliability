{% macro upload_dbt_artifacts() %}
  {% if execute and results %}
    {% set model_upload_func_map = {
      "dbt_models": elementary.upload_dbt_models,
      "dbt_tests": elementary.upload_dbt_tests,
      "dbt_sources": elementary.upload_dbt_sources,
      "dbt_snapshots": elementary.upload_dbt_snapshots,
      "dbt_metrics": elementary.upload_dbt_metrics,
      "dbt_exposures": elementary.upload_dbt_exposures,
      "dbt_seeds": elementary.upload_dbt_seeds,
      }
    %}

    {% set artifacts_state = elementary.get_artifacts_state() %}
    {% do elementary.debug_log("Uploading dbt artifacts.") %}
    {% for artifacts_model, upload_artifacts_func in model_upload_func_map.items() %}
      {% if not elementary.get_result_node(artifacts_model) %}
        {% if elementary.get_elementary_relation(artifacts_model) %}
          {% do upload_artifacts_func(should_commit=true, state_hash=artifacts_state.get(artifacts_model)) %}
        {% endif %}
      {% else %}
        {% do elementary.debug_log('[{}] Artifacts already ran.'.format(artifacts_model)) %}
      {% endif %}
    {% endfor %}
    {% do elementary.debug_log("Uploaded dbt artifacts successfully.") %}
  {% endif %}
{% endmacro %}

{% macro get_artifacts_state() %}
    {# The stored state is only needed if it can be compared later to the local state. #}
    {% if not local_md5 %}
        {% do return({}) %}
    {% endif %}

    {% set database_name, schema_name = elementary.get_package_database_and_schema() %}
    {% set artifacts_state_relation = adapter.get_relation(database_name, schema_name, "dbt_artifacts_state") %}
    {% if not artifacts_state_relation %}
        {% do return({}) %}
    {% endif %}

    {% set stored_artifacts_query %}
    select * from {{ artifacts_state_relation }}
    {% endset %}
    {% do return(dbt.run_query(stored_artifacts_query)[0]) %}
{% endmacro %}
