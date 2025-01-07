{%- macro upload_dbt_columns(should_commit=false, metadata_hashes=none) -%}
    {% set relation = elementary.get_elementary_relation('dbt_columns') %}
    {% if execute and relation %}
        {% set tables = graph.nodes.values() | list + graph.sources.values() | list %}
        {% do elementary.upload_artifacts_to_table(relation, tables, elementary.flatten_table_columns, should_commit=should_commit, metadata_hashes=metadata_hashes) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}

{% macro get_dbt_columns_empty_table_query() %}
    {% set columns = [
        ('unique_id', 'string'),
        ('parent_unique_id', 'string'),
        ('name', 'string'),
        ('data_type', 'string'),
        ('tags', 'long_string'),
        ('meta', 'long_string'),
        ('database_name', 'string'),
        ('schema_name', 'string'),
        ('table_name', 'string'),
        ('description', 'long_string'),
        ('resource_type', 'string'),
        ('generated_at', 'string'),
        ('metadata_hash', 'string'),
    ] %}
    {% set dbt_columns_empty_table_query = elementary.empty_table(columns) %}
    {{ return(dbt_columns_empty_table_query) }}
{% endmacro %}

{% macro flatten_table_columns(table_node) %}
    {% set column_nodes = table_node.get("columns") %}
    {% if not column_nodes %}
        {% do return(none) %}
    {% endif %}

    {% set flattened_columns = [] %}
    {% for column_node in column_nodes.values() %}
        {% set config_dict = elementary.safe_get_with_default(column_node, 'config', {}) %}
        {% set config_meta_dict = elementary.safe_get_with_default(config_dict, 'meta') %}
        {% set meta_dict = elementary.safe_get_with_default(column_node, 'meta', {}) %}
        {% set has_meta = config_meta_dict or meta_dict | length > 0 %}

        {% set config_tags = elementary.safe_get_with_default(config_dict, 'tags') %}
        {% set global_tags = elementary.safe_get_with_default(column_node, 'tags') %}
        {% set meta_tags = elementary.safe_get_with_default(meta_dict, 'tags') %}
        {% set has_tags = config_tags or global_tags or meta_tags %}

        {% if elementary.get_config_var('columns_upload_strategy') == 'all' or column_node.get('description') or has_meta or has_tags %}
            {% set flat_column = elementary.flatten_column(table_node, column_node) %}
            {% do flattened_columns.append(flat_column) %}
        {% endif %}
    {% endfor %}
    {% do return(flattened_columns) %}
{% endmacro %}

{% macro flatten_column(table_node, column_node) %}
    {% set config_dict = elementary.safe_get_with_default(column_node, 'config', {}) %}
    {% set config_meta_dict = elementary.safe_get_with_default(config_dict, 'meta', {}) %}
    {% set meta_dict = elementary.safe_get_with_default(column_node, 'meta', {}) %}
    {% do meta_dict.update(config_meta_dict) %}
    {% set config_tags = elementary.safe_get_with_default(config_dict, 'tags', []) %}
    {% set global_tags = elementary.safe_get_with_default(column_node, 'tags', []) %}
    {% set meta_tags = elementary.safe_get_with_default(meta_dict, 'tags', []) %}
    {% set tags = elementary.union_lists(config_tags, global_tags) %}
    {% set tags = elementary.union_lists(tags, meta_tags) %}
    {% set flatten_column_metadata_dict = {
        'parent_unique_id': table_node.get('unique_id'),
        'unique_id': "column.{}.{}".format(table_node.get('unique_id'), column_node.get('name')),
        'name': column_node.get('name'),
        'data_type': column_node.get('data_type'),
        'tags': elementary.filter_none_and_sort(tags),
        'meta': meta_dict,
        'description': column_node.get('description') or none,
        'database_name': table_node.get('database'),
        'schema_name': table_node.get('schema'),
        'table_name': table_node.get('alias'),
        'resource_type': table_node.get('resource_type'),
        'generated_at': elementary.datetime_now_utc_as_string(),
    } %}
    {% do flatten_column_metadata_dict.update({"metadata_hash": elementary.get_artifact_metadata_hash(flatten_column_metadata_dict)}) %}
    {% do return(flatten_column_metadata_dict) %}
{% endmacro %}
