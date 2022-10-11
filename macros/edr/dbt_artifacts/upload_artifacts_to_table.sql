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


{%- macro render_value_v2(value) -%}
    {%- if value is defined and value is not none -%}
        {%- if value is not string and value is not number-%}
            {{- return(tojson(value)) -}}
        {%- else -%}
            {{- return(value) -}}
        {%- endif -%}
    {%- else -%}
        {{- return(None) -}}
    {%- endif -%}
{%- endmacro -%}


{%- macro upload_artifacts_to_table_v2(table_relation, artifacts, flatten_artifact_callback, artifacts_cached_file_name) -%}
    {% set flatten_artifact_dicts = [] %}
    {% for artifact in artifacts %}
        {% set flatten_artifact_dict = flatten_artifact_callback(artifact) %}
        {% set flatten_artifact_dict_with_only_string_values = dict() %}
        {%- for key in flatten_artifact_dict.keys() -%}
            {% do flatten_artifact_dict_with_only_string_values.update({key: elementary.render_value_v2(flatten_artifact_dict[key])}) %}
        {%- endfor -%}
        {% if flatten_artifact_dict_with_only_string_values is not none %}
            {% do flatten_artifact_dicts.append(flatten_artifact_dict_with_only_string_values) %}
        {% endif %}
    {% endfor %}
    {%- set flatten_artifacts_len = flatten_artifact_dicts | length %}
    {% if flatten_artifacts_len > 0 %}
        {{ elementary.debug_log('Inserting ' ~ flatten_artifacts_len ~ ' rows to table ' ~ table_relation) }}
        {% set empty_agate = run_query("SELECT 1") %}
        {% set flatten_artifacts_agate = empty_agate.from_object(flatten_artifact_dicts) %}
        {% do flatten_artifacts_agate.to_csv(ref.config.target_path ~ '/' ~ artifacts_cached_file_name) %}
        {% set new_flatten_artifacts_agate = flatten_artifacts_agate.from_csv(ref.config.target_path ~ '/' ~ artifacts_cached_file_name) %}
        {# TODO: we need to receive the model node graph using the relation or using the model unique id of the target artifacts table#}
        {% do load_csv_rows(model, new_flatten_artifacts_agate) %}
    {%- else %}
        {{ elementary.debug_log('No artifacts to insert to ' ~ table_relation) }}
    {% endif %}
{%- endmacro -%}