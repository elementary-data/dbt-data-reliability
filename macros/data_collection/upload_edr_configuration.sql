{% macro upload_edr_configuration() %}
    -- depends_on: {{ ref('table_monitors_config') }}
    -- depends_on: {{ ref('column_monitors_config') }}
    {% if execute and results %}
        {% set table_monitors = [] %}
        {% set column_monitors = [] %}
        {% for node in graph.nodes.values() -%}
            {% if node.resource_type == 'seed' %}
                {% do edr_log(node) %}
            {% endif %}
            {% if node.resource_type in ['seed', 'model'] %}
                {% set table_config_dict = get_table_monitors(node) %}
                {% if table_config_dict is not none %}
                    {% do table_monitors.append(table_config_dict) %}
                {% endif %}

                {% set columns_config_dict = get_column_monitors(node) %}
                {% if columns_config_dict is not none%}
                    {% do column_monitors.append(columns_config_dict) %}
                {% endif %}
            {% endif %}
        {% endfor %}
        {% if table_monitors|length > 0 %}
            {% do elementary_data_reliability.insert_dicts_to_table(ref('table_monitors_config'), table_monitors) %}
        {% endif %}
        {% if column_monitors|length > 0 %}
            {% do elementary_data_reliability.insert_dicts_to_table(ref('column_monitors_config'), column_monitors) %}
        {% endif %}
    {% endif %}
    {{ return('') }}
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


{% macro get_table_monitors(node) %}
    {% set table_name = get_table_name(node) %}
    {% set edr_monitored = node.config.get('edr_monitored') %}
    {% if edr_monitored is not none %}
        {% set id = node.database + '.' + node.schema + '.' + table_name %}
        {{ return({'id': id, 'database_name': node.database, 'schema_name': node.schema, 'table_name': table_name, 'timestamp_column': node.config.get('edr_timestamp_column'), 'bucket_duration_hours': 24, 'monitored': edr_monitored, 'monitors': node.config.get('edr_table_monitors')}) }}
    {% else %}
        {{ return(none) }}
    {% endif %}
{% endmacro %}


{% macro get_column_monitors(node) %}
    {% set table_name = get_table_name(node) %}
    {% set edr_columns_monitored = node.config.get('edr_columns_monitored') %}
    {% if edr_columns_monitored is not none %}
        {% set edr_columns = node.config.get('edr_columns') %}
        {% if edr_columns is not none %}
            {% for edr_column in edr_columns %}
                {% if edr_column is mapping %}
                    {% set edr_column_name = edr_column.get('name') %}
                    {% set edr_column_monitors = edr_column.get('monitors') %}
                    {% if edr_column_name is not none %}
                        {% set id = node.database + '.' + node.schema + '.' + table_name + '.' + edr_column_name %}
                        {{ return({'id': id, 'database_name': node.database, 'schema_name': node.schema, 'table_name': table_name, 'column_name': edr_column_name, 'monitored': edr_columns_monitored, 'monitors': edr_column_monitors}) }}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% else %}
            {{ return({'database_name': node.database, 'schema_name': node.schema, 'table_name': table_name, 'column_name': none, 'monitored': edr_columns_monitored, 'monitors': none}) }}
        {% endif %}
    {% else %}
        {{ return(none) }}
    {% endif %}
{% endmacro %}