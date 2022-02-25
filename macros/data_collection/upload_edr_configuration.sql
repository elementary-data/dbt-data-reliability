{% macro upload_edr_configuration() %}
    -- depends_on: {{ ref('table_monitors_config') }}
    -- depends_on: {{ ref('column_monitors_config') }}
    {% if execute and results %}
        {% set table_monitors = [] %}
        {% set column_monitors = [] %}
        {% for source in graph.sources.values() -%}
            {% do edr_log(source.config) %}
        {% endfor %}
        {% for node in graph.nodes.values() -%}
            {% if node.resource_type == 'model' %}
                {% if node.alias %}
                    {% set table_name = node.alias %}
                {% else %}
                    {% set table_name = node.name %}
                {% endif %}
                {% set edr_monitored = node.config.get('edr_monitored') %}
                {% if edr_monitored is not none %}
                    {% do table_monitors.append({'database_name': node.database, 'schema_name': node.schema, 'table_name': table_name, 'timestamp_column': node.config.get('edr_timestamp_column'), 'bucket_duration_hours': 24, 'monitored': edr_monitored, 'monitors': node.config.get('edr_table_monitors')}) %}
                {% endif %}

                {% set edr_columns_monitored = node.config.get('edr_columns_monitored') %}
                {% if edr_columns_monitored is not none %}
                    {% set edr_columns = node.config.get('edr_columns') %}
                    {% if edr_columns is not none %}
                        {% for edr_column in edr_columns %}
                            {% if edr_column is mapping %}
                                {% set edr_column_name = edr_column.get('name') %}
                                {% set edr_column_monitors = edr_column.get('monitors') %}
                                {% if edr_column_name is not none %}
                                    {% do column_monitors.append({'database_name': node.database, 'schema_name': node.schema, 'table_name': table_name, 'column_name': edr_column_name, 'monitored': edr_columns_monitored, 'monitors': edr_column_monitors}) %}
                                {% endif %}
                            {% endif %}
                        {% endfor %}
                    {% else %}
                        {% do column_monitors.append({'database_name': node.database, 'schema_name': node.schema, 'table_name': table_name, 'column_name': none, 'monitored': edr_columns_monitored, 'monitors': none}) %}
                    {% endif %}
                {% endif %}
            {% endif %}
        {% endfor %}
        {% do elementary_data_reliability.insert_dicts_to_table(ref('table_monitors_config'), table_monitors) %}
        {% do elementary_data_reliability.insert_dicts_to_table(ref('column_monitors_config'), column_monitors) %}
    {% endif %}
    {{ return('') }}
{% endmacro %}

