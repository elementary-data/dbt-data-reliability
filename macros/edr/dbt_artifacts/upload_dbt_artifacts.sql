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

    {% set artifacts_hashes = elementary.get_artifacts_hashes() %}
    {% do elementary.file_log("Uploading dbt artifacts.") %}
    {% for artifacts_model, upload_artifacts_func in model_upload_func_map.items() %}
      {% if not elementary.get_result_node(artifacts_model) %}
        {% if elementary.get_elementary_relation(artifacts_model) %}
          {% if artifacts_hashes %}
            {% do upload_artifacts_func(should_commit=true, metadata_hashes=artifacts_hashes.get(artifacts_model, [])) %}
          {% else %}
            {% do upload_artifacts_func(should_commit=true) %}
          {% endif %}
        {% endif %}
      {% else %}
        {% do elementary.file_log('[{}] Artifacts already ran.'.format(artifacts_model)) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro get_artifacts_hashes() %}
    {# The stored hashes are only needed if it can be compared later to the local hashes. #}
    {% if not local_md5 %}
        {% do return(none) %}
    {% endif %}

    {% set database_name, schema_name = elementary.get_package_database_and_schema() %}
    {% set artifacts_hash_relation = adapter.get_relation(database_name, schema_name, "dbt_artifacts_hashes") %}
    {% if not artifacts_hash_relation %}
        {% do return(none) %}
    {% endif %}

    {% set stored_artifacts_query %}
    select artifacts_model, metadata_hash from {{ artifacts_hash_relation }}
    order by metadata_hash
    {% endset %}
    {% set artifacts_hashes_results = elementary.run_query(stored_artifacts_query) %}
    {% set artifact_agate_hashes = artifacts_hashes_results.group_by("artifacts_model") %}
    {% set artifacts_hashes = {} %}
    {% for artifacts_model, metadata_hashes in artifact_agate_hashes.items() %}
        {% do artifacts_hashes.update({artifacts_model: metadata_hashes.columns["metadata_hash"]}) %}
    {% endfor %}
    {% do return(artifacts_hashes) %}
{% endmacro %}
