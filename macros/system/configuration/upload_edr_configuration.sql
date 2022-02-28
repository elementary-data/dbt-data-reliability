{% macro upload_tables_configuration() %}
    {% if execute %}
        {% set nodes = elementary.get_nodes_from_graph() %}
        {% set table_monitors = [] %}
        {% for node in nodes | selectattr('resource_type', 'in', 'seed,source,model') -%}
            {% set table_config_dict = elementary.get_table_config(node) %}
            {% if table_config_dict is not none %}
                {% do table_monitors.append(table_config_dict) %}
            {% endif %}
        {% endfor %}
        {% if table_monitors|length > 0 %}
            {% do elementary.insert_dicts_to_table(this, table_monitors) %}
        {% endif %}
        -- remove empty rows created by dbt's materialization
        {% do elementary.remove_empty_rows(this) %}
    {% endif %}
    {{ return('') }}
{% endmacro %}

{% macro upload_columns_configuration() %}
    {% if execute %}
        {% set nodes = elementary.get_nodes_from_graph() %}
        {% set column_monitors = [] %}
        {% for node in nodes | selectattr('resource_type', 'in', 'seed,source,model') -%}
            {% set columns_config_dict = elementary.get_columns_config(node) %}
            {% if columns_config_dict is not none %}
                {% do column_monitors.append(columns_config_dict) %}
            {% endif %}
        {% endfor %}
        {% if column_monitors | length > 0 %}
            {% do elementary.insert_dicts_to_table(this, column_monitors) %}
        {% endif %}
        -- remove empty rows created by dbt's materialization
        {% do elementary.remove_empty_rows(this) %}
    {% endif %}
    {{ return('') }}
{% endmacro %}

{% macro get_nodes_from_graph() %}
    {% set nodes = [] %}
    {% do nodes.extend(graph.sources.values()) %}
    {% do nodes.extend(graph.nodes.values()) %}
    {{ return(nodes) }}
{% endmacro %}

{% macro get_table_name(node) %}
    {% if node.identifier %}
        {% set table_name = node.identifier %}
    {% elif node.alias %}
        {% set table_name = node.alias %}
    {% else %}
        {% set table_name = node.name %}
    {% endif %}
    {{ return(table_name) }}
{% endmacro %}

{% macro get_edr_config_in_columns(node) %}
    {% set edr_config_in_columns = [] %}
    {% if 'columns' in node %}
        {% set columns = node.columns %}
        {% if columns is mapping %}
            {% for column in columns.values() %}
                {% if column is mapping %}
                    {% set column_meta = column.get('meta') %}
                    {% if column_meta is mapping %}
                        {% set column_edr_config = column_meta.get('edr') %}
                        {% if column_edr_config is mapping %}
                            {% do column_edr_config.update({'name': column.get('name')}) %}
                            {% do edr_config_in_columns.append(column_edr_config) %}
                        {% endif %}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
    {{ return(edr_config_in_columns) }}
{% endmacro %}

{% macro get_edr_config(node) %}
    {% set res = {} %}
    {% set edr_config = node.config.get('edr') %}
    {% if edr_config is mapping %}
        {% do res.update(edr_config) %}
    {% endif %}
    {% set config_meta = node.config.get('meta') %}
    {% if config_meta is mapping %}
        {% set edr_config = config_meta.get('edr') %}
        {% if edr_config is mapping %}
            {% do res.update(edr_config) %}
        {% endif %}
    {% endif %}
    {% set edr_config = node.meta.get('edr') %}
    {% if edr_config is mapping %}
        {% do res.update(edr_config) %}
    {% endif %}
    {% set edr_config_in_columns = elementary.get_edr_config_in_columns(node) %}
    {% if edr_config_in_columns | length > 0 %}
        {% do res.update({'columns': edr_config_in_columns}) %}
    {% endif %}
    {{ return(res) }}
{% endmacro %}

{% macro get_table_config(node) %}
    {% set table_name = elementary.get_table_name(node) %}
    {% set edr_config = elementary.get_edr_config(node) %}
    {% if edr_config is mapping %}
        {% set table_monitored = edr_config.get('monitored') %}
        {% set columns_monitored = edr_config.get('columns_monitored') %}
        {% if table_monitored is not none or columns_monitored is not none %}
            {% set full_name = node.database + '.' + node.schema + '.' + table_name %}
            {{ return({'full_table_name': full_name, 'database_name': node.database, 'schema_name': node.schema, 'table_name': table_name, 'timestamp_column': edr_config.get('timestamp_column'), 'bucket_duration_hours': 24, 'table_monitored': table_monitored, 'table_monitors': edr_config.get('table_monitors'), 'columns_monitored': columns_monitored}) }}
        {% endif %}
    {% endif %}
    {{ return(none) }}
{% endmacro %}

{% macro get_columns_config(node) %}
    {% set table_name = elementary.get_table_name(node) %}
    {% set edr_config = elementary.get_edr_config(node) %}
    {% if edr_config is mapping %}
        {% set columns = edr_config.get('columns') %}
        {% if columns is sequence %}
            {% for column in columns %}
                {% if column is mapping %}
                    {% set column_name = column.get('name') %}
                    {% set column_monitors = column.get('column_monitors') %}
                    {% if column_name is not none %}
                        {% set full_name = node.database + '.' + node.schema + '.' + table_name + '.' + column_name %}
                        {{ return({'full_column_name': full_name, 'database_name': node.database, 'schema_name': node.schema, 'table_name': table_name, 'column_name': column_name, 'column_monitors': column_monitors}) }}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
    {{ return(none) }}
{% endmacro %}