{% macro upload_artifacts_to_table(table_relation, artifacts, flatten_artifact_callback, should_commit=False) %}
    {% set flatten_artifact_dicts = [] %}
    {% for artifact in artifacts %}
        {% set flatten_artifact_dict = flatten_artifact_callback(artifact) %}
        {% if flatten_artifact_dict is not none %}
            {% do flatten_artifact_dicts.append(flatten_artifact_dict) %}
        {% endif %}
    {% endfor %}
    {%- set flatten_artifacts_len = flatten_artifact_dicts | length %}
    {% if flatten_artifacts_len > 0 %}
        {{ elementary.debug_log('Inserting ' ~ flatten_artifacts_len ~ ' rows to table ' ~ table_relation) }}
        {% do elementary.insert_dicts(table_relation, flatten_artifact_dicts, elementary.get_config_var('dbt_artifacts_chunk_size'), should_commit) %}
    {%- else %}
        {{ elementary.debug_log('No artifacts to insert to ' ~ table_relation) }}
    {% endif %}
    -- remove empty rows
    {% do elementary.remove_empty_rows(table_relation) %}
    {%- if should_commit -%}
        {% do adapter.commit() %}
    {%- endif -%}
{% endmacro %}