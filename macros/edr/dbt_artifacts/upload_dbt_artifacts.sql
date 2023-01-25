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
          {% do upload_artifacts_func(should_commit=true, state_hashes=artifacts_state.get(artifacts_model)) %}
        {% endif %}
      {% else %}
        {% do elementary.debug_log('[{}] Artifacts already ran.'.format(artifacts_model)) %}
      {% endif %}
    {% endfor %}
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
    select artifacts_model, artifact_hash from {{ artifacts_state_relation }}
    order by artifact_hash
    {% endset %}
    {% set artifacts_state_results = elementary.run_query(stored_artifacts_query) %}
    {% set artifact_agate_hashes = artifacts_state_results.group_by("artifacts_model").select("artifact_hash") %}
    {% set artifacts_hashes = {} %}
    {% for artifacts_model, artifact_hashes in artifact_agate_hashes.items() %}
        {% do artifacts_hashes.update({artifacts_model: artifact_hashes.columns[0]}) %}
    {% endfor %}
    {% do return(artifacts_hashes) %}
{% endmacro %}
