{% macro upload_artifacts_to_table(table_relation, artifacts, flatten_artifact_callback, append=False, should_commit=False, metadata_hashes=None) %}
    {% set flatten_artifact_dicts = [] %}
    {% for artifact in artifacts %}
        {% set flatten_artifact_dict = flatten_artifact_callback(artifact) %}
        {% if flatten_artifact_dict is not none %}
            {% do flatten_artifact_dicts.append(flatten_artifact_dict) %}
        {% endif %}
    {% endfor %}

    {% if metadata_hashes and elementary.get_config_var("cache_artifacts") %}
        {% set artifacts_hashes = flatten_artifact_dicts | map(attribute="metadata_hash") | sort %}
        {% if artifacts_hashes == metadata_hashes %}
            {% do elementary.debug_log("[{}] Artifacts did not change.".format(table_relation.identifier)) %}
            {% do return(none) %}
        {% endif %}
    {% endif %}

    {% if append %}
        {# In append mode, just insert, and no need to be atomic #}
        {% do elementary.insert_rows(table_relation, flatten_artifact_dicts, should_commit, elementary.get_config_var('dbt_artifacts_chunk_size')) %}
    {% else %}
        {% do elementary.replace_table_data(table_relation, flatten_artifact_dicts) %}
    {% endif %}
{% endmacro %}
