{%- macro upload_dbt_groups(should_commit=false, metadata_hashes=none) -%}
    {% set relation = elementary.get_elementary_relation('dbt_groups') %}
    {% if execute and relation %}
        {% set groups = graph.groups.values() | selectattr('resource_type', '==', 'group') %}
        {% do elementary.upload_artifacts_to_table(relation, groups, elementary.flatten_group, should_commit=should_commit, metadata_hashes=metadata_hashes) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}

{% macro get_dbt_groups_empty_table_query() %}
    {% set columns = [
        ('unique_id', 'string'),
        ('name', 'string'),
        ('owner_email', 'string'),
        ('owner_name', 'string'),
        ('generated_at', 'string'),
        ('metadata_hash', 'string'),
    ] %}

    {% set dbt_groups_empty_table_query = elementary.empty_table(columns) %}
    {{ return(dbt_groups_empty_table_query) }}
{% endmacro %}

{% macro flatten_group(node_dict) %}
    {% set owner_dict = elementary.safe_get_with_default(node_dict, 'owner', {}) %}

    {% set flatten_group_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'name': node_dict.get('name'),
        'owner_email': owner_dict.get('email'),
        'owner_name': owner_dict.get('name'),
        'generated_at': elementary.datetime_now_utc_as_string(),
    } %}
    {% do flatten_group_metadata_dict.update({'metadata_hash': elementary.get_artifact_metadata_hash(flatten_group_metadata_dict)}) %}
    {{ return(flatten_group_metadata_dict) }}
{% endmacro %}
