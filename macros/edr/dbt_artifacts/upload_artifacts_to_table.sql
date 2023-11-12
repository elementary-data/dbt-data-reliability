{% macro upload_artifacts_to_table(table_relation, artifacts, flatten_artifact_callback, append=False, should_commit=False, metadata_hashes=None, on_query_exceed=none) %}
    {% set flatten_artifact_dicts = [] %}
    {% do elementary.file_log("[{}] Flattening the artifacts.".format(table_relation.identifier)) %}
    {% for artifact in artifacts %}
        {% set flatten_artifact = flatten_artifact_callback(artifact) %}
        {% if flatten_artifact is mapping %}
            {% do flatten_artifact_dicts.append(flatten_artifact) %}
        {% elif flatten_artifact is iterable %}
            {% do flatten_artifact_dicts.extend(flatten_artifact) %}
        {% endif %}
    {% endfor %}
    {% do elementary.file_log("[{}] Flattened {} artifacts.".format(table_relation.identifier, flatten_artifact_dicts | length)) %}

    {% if append %}
        {# In append mode, just insert, and no need to be atomic #}
        {% do elementary.insert_rows(table_relation, flatten_artifact_dicts, should_commit, elementary.get_config_var('dbt_artifacts_chunk_size'), on_query_exceed) %}
    {% else %}
        {% if metadata_hashes is not none and elementary.get_config_var("cache_artifacts") %}
            {% do elementary.file_log("[{}] Comparing the artifacts state.".format(table_relation.identifier)) %}
            {% set new_metadata_hashes = flatten_artifact_dicts | map(attribute="metadata_hash") | sort %}
            {% if new_metadata_hashes == metadata_hashes %}
                {% do elementary.file_log("[{}] Artifacts did not change.".format(table_relation.identifier)) %}
            {% else %}
                {% do elementary.file_log("[{}] Artifacts changed.".format(table_relation.identifier)) %}
                {% set upload_artifacts_method = elementary.get_config_var("upload_artifacts_method") %}
                {% if upload_artifacts_method == "diff" %}
                    {% set added_artifacts = flatten_artifact_dicts | rejectattr("metadata_hash", "in", metadata_hashes) | list %}
                    {% set removed_artifact_hashes = metadata_hashes | reject("in", new_metadata_hashes) | list %}
                    {% do elementary.delete_and_insert(table_relation, insert_rows=added_artifacts, delete_values=removed_artifact_hashes, delete_column_key="metadata_hash") %}
                {% elif upload_artifacts_method == "replace" %}
                    {% do elementary.replace_table_data(table_relation, flatten_artifact_dicts) %}
                {% else %}
                    {% do exceptions.raise_compiler_error("Invalid var('upload_artifacts_method') provided.") %}
                {% endif %}
            {% endif %}
        {% else %}
            {% do elementary.replace_table_data(table_relation, flatten_artifact_dicts) %}
        {% endif %}
    {% endif %}
{% endmacro %}
