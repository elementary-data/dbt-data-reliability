{%- macro upload_dbt_models(should_commit=false, cache=true) -%}
    {% set relation = elementary.get_elementary_relation('dbt_models') %}
    {% if execute and relation %}
        {% set models = graph.nodes.values() | selectattr('resource_type', '==', 'model') %}
        {% do elementary.upload_artifacts_to_table(relation, models, elementary.flatten_model, should_commit=should_commit, cache=cache) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}

{% macro get_dbt_models_empty_table_query() %}
    {% set dbt_models_empty_table_query = elementary.empty_table([('unique_id', 'string'),
                                                                  ('alias', 'string'),
                                                                  ('checksum', 'string'),
                                                                  ('materialization', 'string'),
                                                                  ('tags', 'long_string'),
                                                                  ('meta', 'long_string'),
                                                                  ('owner', 'string'),
                                                                  ('database_name', 'string'),
                                                                  ('schema_name', 'string'),
                                                                  ('depends_on_macros', 'long_string'),
                                                                  ('depends_on_nodes', 'long_string'),
                                                                  ('description', 'long_string'),
                                                                  ('name', 'string'),
                                                                  ('package_name', 'string'),
                                                                  ('original_path', 'long_string'),
                                                                  ('path', 'string'),
                                                                  ('generated_at', 'string')]) %}
    {{ return(dbt_models_empty_table_query) }}
{% endmacro %}

{% macro flatten_model(node_dict) %}
    {% set checksum_dict = elementary.safe_get_with_default(node_dict, 'checksum', {}) %}
    {% set config_dict = elementary.safe_get_with_default(node_dict, 'config', {}) %}
    {% set depends_on_dict = elementary.safe_get_with_default(node_dict, 'depends_on', {}) %}

    {% set config_meta_dict = elementary.safe_get_with_default(config_dict, 'meta', {}) %}
    {% set meta_dict = elementary.safe_get_with_default(node_dict, 'meta', {}) %}
    {% do meta_dict.update(config_meta_dict) %}
    {% set formatted_owner = [] %}
    {% set raw_owner = meta_dict.get('owner') %}
    {% if raw_owner is string %}
        {% set owners = raw_owner.split(',') %}
        {% for owner in owners %}
            {% do formatted_owner.append(owner | trim) %}  
        {% endfor %}
    {% elif raw_owner is iterable %}
        {% do formatted_owner.extend(raw_owner) %}
    {% endif %}
    {% set config_tags = elementary.safe_get_with_default(config_dict, 'tags', []) %}
    {% set global_tags = elementary.safe_get_with_default(node_dict, 'tags', []) %}
    {% set meta_tags = elementary.safe_get_with_default(meta_dict, 'tags', []) %}
    {% set tags = elementary.union_lists(config_tags, global_tags) %}
    {% set tags = elementary.union_lists(tags, meta_tags) %}

    {% set flatten_model_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'alias': node_dict.get('alias'),
        'checksum': checksum_dict.get('checksum'),
        'materialization': config_dict.get('materialized'),
        'tags': tags,
        'meta': meta_dict,
        'owner': formatted_owner,
        'database_name': node_dict.get('database'),
        'schema_name': node_dict.get('schema'),
        'depends_on_macros': depends_on_dict.get('macros', []),
        'depends_on_nodes': depends_on_dict.get('nodes', []),
        'description': node_dict.get('description'),
        'name': node_dict.get('name'),
        'package_name': node_dict.get('package_name'),
        'original_path': node_dict.get('original_file_path'),
        'path': node_dict.get('path'),
        'generated_at': elementary.datetime_now_utc_as_string()
    }%}
    {{ return(flatten_model_metadata_dict) }}
{% endmacro %}