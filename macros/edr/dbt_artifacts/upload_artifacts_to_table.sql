{% macro upload_artifacts_to_table(table_relation, artifacts, flatten_artifact_callback, truncate_if_on_run_end, should_commit=False) %}
    {% set flatten_artifact_dicts = [] %}
    {% for artifact in artifacts %}
        {% set flatten_artifact_dict = flatten_artifact_callback(artifact) %}
        {% if flatten_artifact_dict is not none %}
            {% do flatten_artifact_dicts.append(flatten_artifact_dict) %}
        {% endif %}
    {% endfor %}
    {% if truncate_if_on_run_end and elementary.is_on_run_end() %}
      {% do dbt.truncate_relation(table_relation) %}
    {% endif %}
    {% do elementary.insert_rows(table_relation, flatten_artifact_dicts, should_commit, elementary.get_config_var('dbt_artifacts_chunk_size')) %}
    -- remove empty rows
    {% do elementary.remove_empty_rows(table_relation) %}
    {%- if should_commit -%}
        {% do adapter.commit() %}
    {%- endif -%}
{% endmacro %}
