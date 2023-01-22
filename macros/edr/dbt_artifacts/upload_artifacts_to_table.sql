{% macro upload_artifacts_to_table(table_relation, artifacts, flatten_artifact_callback, append=False, should_commit=False, state_hash=None) %}
    {% set flatten_artifact_dicts = [] %}
    {% for artifact in artifacts %}
        {% set flatten_artifact_dict = flatten_artifact_callback(artifact) %}
        {% if flatten_artifact_dict is not none %}
            {% do flatten_artifact_dicts.append(flatten_artifact_dict) %}
        {% endif %}
    {% endfor %}

    {% if local_md5 and state_hash %}
        {% set artifacts_hash = local_md5(flatten_artifact_dicts | map(attribute="artifact_hash") | sort | join(",")) %}
        {% if artifacts_hash == state_hash %}
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
