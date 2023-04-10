{% macro upload_artifacts_to_table(table_relation, artifacts, flatten_artifact_callback, append=False, should_commit=False, metadata_hashes=None) %}
    {% if append %}
        {% set flattened_artifacts = elementary.get_flattened_artifacts(table_relation, artifacts, flatten_artifact_callback) %}
        {% do elementary.insert_rows(table_relation, flattened_artifacts, should_commit, elementary.get_config_var('dbt_artifacts_chunk_size')) %}
    {% else %}
        {% if metadata_hashes is not none and elementary.get_config_var("cache_artifacts") %}
            {% do elementary.file_log("[{}] Comparing the artifacts state.".format(table_relation.identifier)) %}
            {% set artifacts = elementary.get_flattened_new_artifacts(table_relation, artifacts, flatten_artifact_callback, metadata_hashes) %}
            {% set flattened_new_artifacts = artifacts.flattened_new_artifacts %}
            {% set removed_metadata_hashes = artifacts.removed_metadata_hashes %}
            {% if not flattened_new_artifacts and not removed_metadata_hashes %}
                {% do elementary.file_log("[{}] Artifacts did not change.".format(table_relation.identifier)) %}
            {% else %}
                {% do elementary.file_log("[{}] Artifacts changed.".format(table_relation.identifier)) %}
                {% do elementary.delete_and_insert(table_relation, insert_rows=flattened_new_artifacts, delete_values=removed_metadata_hashes, delete_column_key="metadata_hash") %}
            {% endif %}
        {% else %}
            {% set flattened_artifacts = elementary.get_flattened_artifacts(table_relation, artifacts, flatten_artifact_callback) %}
            {% do elementary.replace_table_data(table_relation, flattened_artifacts) %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro get_flattened_artifacts(table_relation, artifacts, flatten_artifact_callback) %}
    {% set flattened_new_artifacts = [] %}
    {% for artifact in artifacts %}
        {% do flattened_new_artifacts.append(flatten_artifact_callback(artifact)) %}
    {% endfor %}
    {% do elementary.file_log("[{}] Flattened {} artifacts.".format(table_relation.identifier, flattened_new_artifacts | length)) %}
    {% do return(flattened_new_artifacts) %}
{% endmacro %}

{% macro get_flattened_new_artifacts(table_relation, artifacts, flatten_artifact_callback, metadata_hashes) %}
    {% set current_metadata_hashes = [] %}
    {% set flattened_new_artifacts = [] %}
    {% for artifact in artifacts %}
        {% set artifact_metadata_hash = elementary.get_artifact_metadata_hash(artifact) %}
        {% do current_metadata_hashes.append(artifact_metadata_hash) %}
        {% if artifact_metadata_hash not in metadata_hashes %}
            {% do flattened_new_artifacts.append(flatten_artifact_callback(artifact)) %}
        {% endif %}
    {% endfor %}
    {% do elementary.file_log("[{}] Flattened {} new artifacts.".format(table_relation.identifier, flattened_new_artifacts | length)) %}
    {% set removed_metadata_hashes = metadata_hashes | reject("in", current_metadata_hashes) | list %}
    {% do return({"flattened_new_artifacts": flattened_new_artifacts, "removed_metadata_hashes": removed_metadata_hashes}) %}
{% endmacro %}
