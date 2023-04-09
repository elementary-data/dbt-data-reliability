{% macro upload_artifacts_to_table(table_relation, artifacts, flatten_artifact_callback, append=False, should_commit=False, metadata_hashes=None) %}
    {% set flatten_artifact_dicts = [] %}
    {% do elementary.file_log("[{}] Flattening the artifacts.".format(table_relation.identifier)) %}
    {% for artifact in artifacts %}
        {% set flatten_artifact_dict = flatten_artifact_callback(artifact) %}
        {% if flatten_artifact_dict is not none %}
            {% do flatten_artifact_dicts.append(flatten_artifact_dict) %}
        {% endif %}
    {% endfor %}
    {% do elementary.file_log("[{}] Flattened the artifacts.".format(table_relation.identifier)) %}

    {% if metadata_hashes is not none and elementary.get_config_var("cache_artifacts") %}
        {% do elementary.file_log("[{}] Comparing the artifacts state.".format(table_relation.identifier)) %}
        {% set artifacts_hashes = flatten_artifact_dicts | map(attribute="metadata_hash") | sort %}
        {% if artifacts_hashes == metadata_hashes %}
            {% do elementary.file_log("[{}] Artifacts did not change.".format(table_relation.identifier)) %}
            {% do return(none) %}
        {% else %}
            {% do elementary.file_log("[{}] Artifacts changed.".format(table_relation.identifier)) %}
        {% endif %}
    {% endif %}

    {% if append %}
        {# In append mode, just insert, and no need to be atomic #}
        {% do elementary.insert_rows(table_relation, flatten_artifact_dicts, should_commit, elementary.get_config_var('dbt_artifacts_chunk_size')) %}
    {% else %}
        {% if metadata_hashes is not none %}
            {% set added_artifacts = flatten_artifact_dicts | rejectattr("metadata_hash", "in", metadata_hashes) | list %}
            {% set removed_artifact_hashes = metadata_hashes | reject("in", artifacts_hashes) | list %}
            {% do elementary.delete_and_insert(table_relation, insert_rows=added_artifacts, delete_values=removed_artifact_hashes, delete_column_key="metadata_hash") %}
        {% else %}
            {% do elementary.replace_table_data(table_relation, flatten_artifact_dicts) %}
        {% endif %}
    {% endif %}
{% endmacro %}
