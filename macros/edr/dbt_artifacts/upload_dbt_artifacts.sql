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
      "dbt_groups": elementary.upload_dbt_groups,
      }
    %}

    {% if elementary.get_config_var('columns_upload_strategy') != 'none' %}
      {% do model_upload_func_map.update({"dbt_columns": elementary.upload_dbt_columns}) %}
    {% endif %}

    {% set artifacts_hashes = elementary.get_artifacts_hashes() %}
    {% do elementary.file_log("Uploading dbt artifacts.") %}
    {% for artifacts_model, upload_artifacts_func in model_upload_func_map.items() %}
      {% if not elementary.get_result_node(artifacts_model) %}
        {% if elementary.get_elementary_relation(artifacts_model) %}
          {% if artifacts_hashes is not none %}
            {% do upload_artifacts_func(should_commit=true, metadata_hashes=artifacts_hashes.get(artifacts_model, [])) %}
          {% else %}
            {% do upload_artifacts_func(should_commit=true) %}
          {% endif %}
        {% endif %}
      {% else %}
        {% do elementary.file_log('[{}] Artifacts already ran.'.format(artifacts_model)) %}
      {% endif %}
    {% endfor %}
    {% do elementary.file_log("Uploaded dbt artifacts.") %}
  {% endif %}
{% endmacro %}

{% macro get_artifacts_hashes() %}
    {# The stored hashes are only needed if it can be compared later to the local hashes. #}
    {% if not local_md5 %}
        {% do return(none) %}
    {% endif %}

    {% set artifacts_hash_relation = elementary.get_elementary_relation('dbt_artifacts_hashes') %}
    {% if not artifacts_hash_relation %}
        {% do return(none) %}
    {% endif %}

    {% set stored_artifacts_query %}
    select artifacts_model, metadata_hash from {{ artifacts_hash_relation }}
    order by metadata_hash
    {% endset %}
    {% set artifacts_hashes_results = elementary.run_query(stored_artifacts_query) %}
    {% set artifacts_hashes = elementary.agate_to_dicts(artifacts_hashes_results) %}

    {% set artifacts_hashes_per_model = {} %}
    {% for artifacts_hashes_row in artifacts_hashes %}
        {% do artifacts_hashes_per_model.setdefault(artifacts_hashes_row['artifacts_model'], []) %}
        {% do artifacts_hashes_per_model[artifacts_hashes_row['artifacts_model']].append(artifacts_hashes_row['metadata_hash']) %}
    {% endfor %}
    
    {% do return(artifacts_hashes_per_model) %}
{% endmacro %}

{% macro get_artifacts_hashes_for_model(model_name) %}
    {% if not local_md5 %}
        {% do return(none) %}
    {% endif %}

    {% set stored_artifacts_query %}
    select metadata_hash 
    from {{ elementary.get_elementary_relation(model_name) }}
    order by metadata_hash
    {% endset %}

    {% set artifacts_hashes_results = elementary.run_query(stored_artifacts_query) %}
    {% do return(artifacts_hashes_results.columns["metadata_hash"]) %}
{% endmacro %}
