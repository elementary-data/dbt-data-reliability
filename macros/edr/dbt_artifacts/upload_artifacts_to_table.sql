{% macro upload_artifacts_to_table(table_relation, artifacts, flatten_artifact_callback, append=False, should_commit=False, metadata_hashes=None, on_query_exceed=none) %}
    {% set context_name = 'upload_artifacts_to_table[' ~ table_relation.name ~ ']'%}
    {% do elementary.begin_duration_measure_context(context_name) %}
    {% set flatten_artifact_dicts = [] %}
    {% do elementary.file_log("[{}] Flattening the artifacts.".format(table_relation.identifier)) %}
    {% do elementary.begin_duration_measure_context('artifacts_flatten') %}
    {% for artifact in artifacts %}
        {% set flatten_artifact = flatten_artifact_callback(artifact) %}
        {% if flatten_artifact is mapping %}
            {% do flatten_artifact_dicts.append(flatten_artifact) %}
        {% elif flatten_artifact is iterable %}
            {% do flatten_artifact_dicts.extend(flatten_artifact) %}
        {% endif %}
    {% endfor %}
    {% do elementary.end_duration_measure_context('artifacts_flatten') %}
    {% do elementary.file_log("[{}] Flattened {} artifacts.".format(table_relation.identifier, flatten_artifact_dicts | length)) %}

    {% if append %}
        {# In append mode, just insert, and no need to be atomic #}

        {% do elementary.insert_rows(table_relation, flatten_artifact_dicts, should_commit, elementary.get_config_var('dbt_artifacts_chunk_size'), on_query_exceed) %}
    {% else %}

        {% set upload_artifact_method = get_upload_artifact_method(table_relation, metadata_hashes) %}
        {% if upload_artifact_method.type == "diff" %}
            {% do elementary.file_log("[{}] Comparing the artifacts state.".format(table_relation.identifier)) %}
            {% set new_metadata_hashes = flatten_artifact_dicts | map(attribute="metadata_hash") | sort %}
            {% if new_metadata_hashes == upload_artifact_method.metadata_hashes %}
                {% do elementary.file_log("[{}] Artifacts did not change.".format(table_relation.identifier)) %}
            {% else %}
                {% do elementary.file_log("[{}] Artifacts changed.".format(table_relation.identifier)) %}
                {% set added_artifacts = flatten_artifact_dicts | rejectattr("metadata_hash", "in", upload_artifact_method.metadata_hashes) | list %}
                {% set removed_artifact_hashes = upload_artifact_method.metadata_hashes | reject("in", new_metadata_hashes) | list %}
                {% do elementary.delete_and_insert(table_relation, insert_rows=added_artifacts, delete_values=removed_artifact_hashes, delete_column_key="metadata_hash") %}
            {% endif %}
        {% else %}
            {% do elementary.replace_table_data(table_relation, flatten_artifact_dicts) %}
        {% endif %}
    {% endif %}

    {% do elementary.end_duration_measure_context(context_name, log_durations=true) %}
{% endmacro %}

{% macro get_upload_artifact_method(table_relation, metadata_hashes) %}
    {% if not elementary.get_config_var("cache_artifacts") %}
        {% do return({"type": "replace"}) %}
    {% endif %}

    {% set upload_artifacts_method = elementary.get_config_var("upload_artifacts_method") %}
    {% if upload_artifacts_method == 'diff' %}
        {% set metadata_hashes = metadata_hashes if metadata_hashes is not none else elementary.get_artifacts_hashes_for_model(table_relation.name) %}
        {% if metadata_hashes is not none %}
            {% do return({"type": "diff", "metadata_hashes": metadata_hashes}) %}
        {% else %}
            {% do return({"type": "replace"}) %}
        {% endif %}
    {% elif upload_artifacts_method == "replace" %}
        {% do return({"type": "replace"}) %}
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid var('upload_artifacts_method') provided.") %}
    {% endif %}
{% endmacro %}
