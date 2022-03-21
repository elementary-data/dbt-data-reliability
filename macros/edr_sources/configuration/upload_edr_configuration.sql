{% macro upload_edr_configuration() %}
    {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
    {% if execute and not edr_cli_run and flags.WHICH == 'run' %}
        {% set nodes = elementary.get_nodes_from_graph() %}
        {% set test_nodes = nodes | selectattr('resource_type', '==', 'test') %}
        {% set config_in_tests = elementary.get_config_from_tests(test_nodes) %}
        {% do elementary.upload_tables_configuration(nodes, config_in_tests) %}
        {% do elementary.upload_columns_configuration(nodes, config_in_tests) %}
    {% endif %}
    {{ return('') }}
{% endmacro %}

{% macro upload_tables_configuration(nodes, config_in_tests) %}
    {% set empty_table_config_query = elementary.empty_table([('full_table_name', 'string'),
                                                              ('database_name', 'string'),
                                                              ('schema_name', 'string'),
                                                              ('table_name', 'string'),
                                                              ('timestamp_column', 'string'),
                                                              ('bucket_duration_hours', 'int'),
                                                              ('table_monitored', 'boolean'),
                                                              ('table_monitors', 'string'),
                                                              ('columns_monitored', 'boolean')]) %}
    {% set table_config_relation = elementary.create_source_table('table_monitors_config', empty_table_config_query, True) %}
    {% set table_monitors = [] %}
    {% for node in nodes | selectattr('resource_type', 'in', 'seed,source,model') -%}
        {% set table_config_dict = elementary.get_table_config(node, config_in_tests) %}
        {% if table_config_dict is not none %}
            {% do table_monitors.append(table_config_dict) %}
        {% endif %}
    {% endfor %}
    {% if table_monitors|length > 0 %}
        {% do elementary.insert_dicts(table_config_relation, table_monitors) %}
    {% endif %}
    -- remove empty rows
    {% do elementary.remove_empty_rows(table_config_relation) %}
{% endmacro %}

{% macro upload_columns_configuration(nodes, config_in_tests) %}
    {% set empty_columns_config_query = elementary.empty_table([('full_column_name', 'string'),
                                                                ('database_name', 'string'),
                                                                ('schema_name', 'string'),
                                                                ('table_name', 'string'),
                                                                ('column_name', 'string'),
                                                                ('column_monitors', 'string')]) %}
    {% set column_config_relation = elementary.create_source_table('column_monitors_config', empty_columns_config_query, True) %}
    {% set column_monitors = [] %}
    {% for node in nodes | selectattr('resource_type', 'in', 'seed,source,model') -%}
        {% do column_monitors.extend(elementary.get_columns_config(node, config_in_tests)) %}
    {% endfor %}
    {% if column_monitors | length > 0 %}
        {% do elementary.insert_dicts(column_config_relation, column_monitors) %}
    {% endif %}
    -- remove empty rows
    {% do elementary.remove_empty_rows(column_config_relation) %}
{% endmacro %}

{% macro get_config_from_tests(test_nodes) %}
    {% set config_in_tests = {} %}
    {% for node in test_nodes %}
        {% set edr_config = elementary.get_elementary_config_from_test(node) %}
        {% if edr_config %}
            {% set test_name = edr_config.get('test_name') %}
            {% set model_unique_id = edr_config.get('model_unique_id') %}
            {% if model_unique_id %}
                {% if model_unique_id not in config_in_tests %}
                    {% do config_in_tests.update({model_unique_id: {'table_monitors': [], 'columns': {}}}) %}
                {% endif %}
                {% if test_name == 'table_anomalies' %}
                    {% set table_monitors = edr_config.get('table_monitors') %}
                    {% if not table_monitors %}
                        {% set table_monitors = [] %}
                    {% endif %}
                    {% do config_in_tests[model_unique_id]['table_monitors'].extend([table_monitors]) %}
                {% elif test_name == 'schema_changes' %}
                    {% do config_in_tests[model_unique_id]['table_monitors'].extend([['schema_changes']]) %}
                {% elif test_name == 'column_anomalies' or 'all_columns_anomalies' %}
                    {% set column_name = edr_config.get('column_name') %}
                    {% if not column_name %}
                        {% set column_name = '__ALL_COLUMNS__'%}
                    {% endif %}
                    {% set column_monitors = edr_config.get('column_monitors') %}
                    {% if not column_monitors %}
                        {% set column_monitors = [] %}
                    {% endif %}
                    {% if column_name in config_in_tests[model_unique_id]['columns'] %}
                        {% do config_in_tests[model_unique_id]['columns'][column_name].extend([column_monitors]) %}
                    {% else %}
                        {% do config_in_tests[model_unique_id]['columns'].update({column_name: [column_monitors]}) %}
                    {% endif %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {{ return(config_in_tests) }}
{% endmacro %}

{% macro find_test_model_unique_id(test_depends_on) %}
    {% set depends_on_nodes = test_depends_on.get('nodes') %}
    {% if depends_on_nodes %}
        {% for node in depends_on_nodes %}
            {% if not node.startswith('model.elementary.') %}
                {{ return(node) }}
            {% endif %}
        {% endfor %}
    {% endif %}
    {{ return(none) }}
{% endmacro %}

{% macro get_elementary_config_from_test(test_node) %}
    {% set edr_config = {} %}
    {% set test_metadata = test_node.get('test_metadata') %}
    {% if test_metadata %}
        {% set test_name = test_metadata.get('name') %}
        {% set test_namespace = test_metadata.get('namespace') %}
        {% set test_params = test_metadata.get('kwargs') %}
        {% if test_params %}
            {% set test_column_name = test_params.get('column_name') %}
            {% set test_column_monitors = test_params.get('column_anomalies') %}
            {% set test_table_monitors = test_params.get('table_anomalies') %}
            {% if test_namespace == 'elementary' %}
                {% set test_model_unique_id = none %}
                {% set test_depends_on = test_node.get('depends_on') %}
                {% if test_depends_on %}
                    {% set test_model_unique_id = elementary.find_test_model_unique_id(test_depends_on) %}
                {% endif %}
                {% if test_model_unique_id %}
                    {% set edr_config = {'model_unique_id': test_model_unique_id,
                                         'table_monitors': test_table_monitors,
                                         'column_name': test_column_name,
                                         'column_monitors': test_column_monitors,
                                         'test_name': test_name} %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endif %}
    {{ return(edr_config) }}
{% endmacro %}

{% macro get_table_config(node, config_in_tests) %}
    {% set node_unique_id = node.get('unique_id') %}
    {% if node_unique_id in config_in_tests %}
        {% set table_monitors = config_in_tests[node_unique_id].get('table_monitors') %}
        {% set columns = config_in_tests[node_unique_id].get('columns') %}
        {% if columns %}
            {% set columns_monitored = True %}
        {% endif %}
        {% set table_name = elementary.get_table_name_from_node(node) %}
        {% set full_table_name = node.database + '.' + node.schema + '.' + table_name %}
        {% set elementary_config = elementary.get_elementary_config_from_node(node) %}
        {% set timestamp_column = elementary_config.get('timestamp_column') %}
        {% if timestamp_column %}
            {% set timestamp_column = timestamp_column | upper %}
        {% endif %}
        {{ return({'full_table_name': full_table_name | upper,
                   'database_name': node.database | upper,
                   'schema_name': node.schema | upper,
                   'table_name': table_name | upper,
                   'timestamp_column': timestamp_column,
                   'bucket_duration_hours': 24,
                   'table_monitored': True,
                   'table_monitors': table_monitors,
                   'columns_monitored': columns_monitored}) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}

{% macro get_columns_config(node, config_in_tests) %}
    {% set column_config_dicts = [] %}
    {% set node_unique_id = node.get('unique_id') %}
    {% if node_unique_id in config_in_tests %}
        {% set columns = config_in_tests[node_unique_id].get('columns') %}
        {% if columns %}
            {% set table_name = elementary.get_table_name_from_node(node) %}
            {% for column_name, column_monitors in columns.items() %}
                {% set full_column_name = node.database + '.' + node.schema + '.' + table_name + '.' + column_name %}
                {% do column_config_dicts.append({'full_column_name': full_column_name | upper,
                                                  'database_name': node.database | upper,
                                                  'schema_name': node.schema | upper,
                                                  'table_name': table_name | upper,
                                                  'column_name': column_name | upper,
                                                  'column_monitors': column_monitors}) %}
            {% endfor %}
        {% endif %}
    {% endif %}
    {{ return(column_config_dicts) }}
{% endmacro %}