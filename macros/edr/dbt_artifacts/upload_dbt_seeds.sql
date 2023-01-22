{%- macro upload_dbt_seeds(should_commit=false) -%}
    {% set relation = elementary.get_elementary_relation('dbt_seeds') %}
    {% if execute and relation %}
        {% set seeds = graph.nodes.values() | selectattr('resource_type', '==', 'seed') %}
        {% do elementary.upload_artifacts_to_table(relation, seeds, elementary.flatten_seed, should_commit=should_commit) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}

{% macro get_dbt_seeds_empty_table_query() %}
    {% set dbt_seeds_empty_table_query = elementary.empty_table([('unique_id', 'string'),
                                                                  ('alias', 'string'),
                                                                  ('checksum', 'string'),
                                                                  ('tags', 'long_string'),
                                                                  ('meta', 'long_string'),
                                                                  ('owner', 'string'),
                                                                  ('database_name', 'string'),
                                                                  ('schema_name', 'string'),
                                                                  ('description', 'long_string'),
                                                                  ('name', 'string'),
                                                                  ('package_name', 'string'),
                                                                  ('original_path', 'long_string'),
                                                                  ('path', 'string'),
                                                                  ('generated_at', 'string'),
                                                                  ('hash', 'string'),
                                                                  ]) %}
    {{ return(dbt_seeds_empty_table_query) }}
{% endmacro %}

{% macro flatten_seed(node_dict) %}
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

    {% set flatten_seed_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'alias': node_dict.get('alias'),
        'checksum': checksum_dict.get('checksum'),
        'tags': tags,
        'meta': meta_dict,
        'owner': formatted_owner,
        'database_name': node_dict.get('database'),
        'schema_name': node_dict.get('schema'),
        'description': node_dict.get('description'),
        'name': node_dict.get('name'),
        'package_name': node_dict.get('package_name'),
        'original_path': node_dict.get('original_file_path'),
        'path': node_dict.get('path'),
        'generated_at': elementary.datetime_now_utc_as_string()
    }%}
    {% do flatten_seed_metadata_dict.update({"hash": local_md5(flatten_seed_metadata_dict | string) if local_md5 else none}) %}
    {{ return(flatten_seed_metadata_dict) }}
{% endmacro %}