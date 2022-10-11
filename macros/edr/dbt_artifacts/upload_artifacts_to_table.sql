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

{% macro upload_csv_artifacts_to_table(table_relation, artifacts, flatten_artifact_callback, should_commit=False) %}
    {% set output_csv_path = elementary.get_target_path(table_relation.identifier ~ '.csv') %}
    {% set flatten_artifact_dicts = [] %}
    {% for artifact in artifacts %}
        {% set flatten_artifact_dict = flatten_artifact_callback(artifact) %}
        {% set flatten_artifact_rendered_dict = {} %}
        {% for key in flatten_artifact_dict %}
          {% set value = flatten_artifact_dict[key] %}
          {% if value %}
            {% do flatten_artifact_rendered_dict.update({key: elementary.render_csv_value(value)}) %}
          {% endif %}
        {% endfor %}
        {% do flatten_artifact_dicts.append(flatten_artifact_rendered_dict) %}
    {% endfor %}
    {% set flatten_artifacts_agate = elementary.get_agate_table().from_object(flatten_artifact_dicts) %}
    {% do flatten_artifacts_agate.to_csv(output_csv_path) %}
    {% do elementary.seed_elementary_model(table_relation, output_csv_path) %}
{% endmacro %}

{%- macro render_csv_value(value) -%}
  {% if not value %}
    {{ return(none) }}
  {% endif %}

  {% if value is not string and value is not number %}
    {{ return(tojson(value)) }}
  {% else %}
    {{ return(value) }}
  {% endif %}
{% endmacro %}
