{% macro insert_nodes_to_table(table_name, nodes, flatten_node_macro) %}
    {% set artifacts = [] %}
    {% for node in nodes %}
        {% set metadata_dict = flatten_node_macro(node) %}
        {% if metadata_dict is not none %}
            {% do artifacts.append(metadata_dict) %}
        {% endif %}
    {% endfor %}
    {%- set artifacts_length = artifacts | length %}
    {% if artifacts_length > 0 %}
        {{ elementary.debug_log('Inserting ' ~ artifacts_length ~ ' rows to table ' ~ table_name) }}
        {% do elementary.insert_dicts(table_name, artifacts, elementary.get_config_var('dbt_artifacts_chunk_size')) %}
    {%- else %}
        {{ elementary.debug_log('No artifacts to insert to ' ~ table_name) }}
    {% endif %}
    -- remove empty rows
    {% do elementary.remove_empty_rows(table_name) %}
{% endmacro %}