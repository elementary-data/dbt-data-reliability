{% macro handle_test_results(results) %}
    {% if execute and flags.WHICH in ['test', 'build'] %}
        {% set test_metrics_tables = [] %}
        {% set anomaly_alerts = [] %}
        {% set schema_change_alerts = [] %}
        {% set dbt_test_alerts = [] %}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% for result in results | selectattr('node.resource_type', '==', 'test') %}
            {% set status = result.status | lower %}
            {% set run_result_dict = result.to_dict() %}
            {% set test_node = elementary.safe_get_with_default(run_result_dict, 'node', {}) %}
            {% set flatten_test_node = elementary.flatten_test(test_node) %}
            {% if flatten_test_node.test_namespace == 'elementary' and status != 'error' %}
                {% set test_row_dicts = [] %}
                {% if status != 'pass' %}
                    {% set test_row_dicts = elementary.get_test_result_rows_as_dicts(flatten_test_node) %}
                {% endif %}
                {% if flatten_test_node.short_name in ['table_anomalies', 'column_anomalies', 'all_columns_anomalies'] %}
                    {% set test_metrics_table = elementary.get_test_metrics_table(database_name, schema_name, flatten_test_node) %}
                    {% if test_metrics_table %}
                        {% do test_metrics_tables.append(test_metrics_table) %}
                    {% endif %}
                    {% for test_row_dict in test_row_dicts %}
                        {% do anomaly_alerts.append(elementary.convert_anomaly_dict_to_alert(database_name,
                                                                                             schema_name,
                                                                                             run_result_dict,
                                                                                             test_row_dict,
                                                                                             flatten_test_node)) %}
                    {% endfor %}
                {% elif flatten_test_node.short_name == 'schema_changes' %}
                    {% for test_row_dict in test_row_dicts %}
                        {% do schema_change_alerts.append(elementary.convert_schema_change_dict_to_alert(run_result_dict,
                                                                                                         test_row_dict,
                                                                                                         flatten_test_node)) %}
                    {% endfor %}
                {% endif %}
            {% elif status != 'pass' %}
                {% do dbt_test_alerts.append(elementary.convert_dbt_test_to_alert(run_result_dict,
                                                                                  flatten_test_node)) %}
            {% endif %}
        {% endfor %}
        {{ elementary.merge_data_monitoring_metrics(database_name, schema_name, test_metrics_tables) }}
        {% do elementary.insert_test_alerts(database_name, schema_name, 'alerts_data_monitoring', anomaly_alerts) %}
        {% do elementary.insert_test_alerts(database_name, schema_name, 'alerts_schema_changes', schema_change_alerts) %}
        {% do elementary.insert_test_alerts(database_name, schema_name, 'alerts_dbt_tests', dbt_test_alerts) %}
    {% endif %}
    {{ return('') }}
{% endmacro %}

{% macro insert_test_alerts(database_name, schema_name, alerts_table_name, alert_list) %}
    {% if alert_list %}
        {%- set alerts_table_relation = adapter.get_relation(database=database_name,
                                                             schema=schema_name,
                                                             identifier=alerts_table_name) %}
        {% do elementary.insert_dicts(alerts_table_relation, alert_list) %}
    {% endif %}
{% endmacro %}

{% macro convert_anomaly_dict_to_alert(elementary_db_name, elementary_schema_name, run_result_dict, anomaly_dict, test_node) %}
    {% set full_table_name = elementary.insensitive_get_dict_value(anomaly_dict, 'full_table_name', '') %}
    {% set split_full_table_name = full_table_name.split('.') %}
    {% set database_name = split_full_table_name[0] %}
    {% set schema_name = split_full_table_name[1] %}
    {% set table_name = split_full_table_name[2] %}
    {% set test_params = elementary.insensitive_get_dict_value(test_node, 'test_params', {}) %}
    {% set anomaly_score_threshold = elementary.insensitive_get_dict_value(anomaly_dict, 'anomaly_score_threshold') %}
    {% set timestamp_column = elementary.insensitive_get_dict_value(anomaly_dict, 'timestamp_column') %}
    {% do test_params.update({'anomaly_score_threshold': anomaly_score_threshold}) %}
    {% do test_params.update({'timestamp_column': timestamp_column}) %}
    {% set column_name = elementary.insensitive_get_dict_value(anomaly_dict, 'column_name') %}
    {% set metric_name = elementary.insensitive_get_dict_value(anomaly_dict, 'metric_name') %}
    {% set training_start = elementary.insensitive_get_dict_value(anomaly_dict, 'training_start') %}
    {% set training_end = elementary.insensitive_get_dict_value(anomaly_dict, 'training_end') %}
    --TODO: change to ref
    {%- set data_monitoring_metrics_relation = elementary.get_data_monitoring_metrics_relation(elementary_db_name, elementary_schema_name) -%}
    {% set alert_results_query = elementary.get_training_set_query(full_table_name, column_name, metric_name, training_start, training_end, data_monitoring_metrics_relation) %}
    {% set alert_dict = {
        'alert_id': elementary.insensitive_get_dict_value(anomaly_dict, 'id'),
        'data_issue_id': elementary.insensitive_get_dict_value(anomaly_dict, 'metric_id'),
        'test_execution_id': elementary.insensitive_get_dict_value(anomaly_dict, 'test_execution_id'),
        'test_unique_id': elementary.insensitive_get_dict_value(anomaly_dict, 'test_unique_id'),
        'model_unique_id': elementary.insensitive_get_dict_value(test_node, 'parent_model_unique_id'),
        'detected_at': elementary.insensitive_get_dict_value(anomaly_dict, 'detected_at'),
        'database_name': database_name,
        'schema_name': schema_name,
        'table_name': table_name,
        'column_name': column_name,
        'alert_type': 'anomaly_detection',
        'sub_type': metric_name,
        'alert_description': elementary.insensitive_get_dict_value(anomaly_dict, 'anomaly_description'),
        'other': elementary.insensitive_get_dict_value(anomaly_dict, 'anomalous_value'),
        'owners': elementary.insensitive_get_dict_value(test_node, 'model_owners'),
        'tags': elementary.insensitive_get_dict_value(test_node, 'model_tags'),
        'alert_results_query': alert_results_query,
        'test_name': elementary.insensitive_get_dict_value(test_node, 'short_name'),
        'test_params': elementary.insensitive_get_dict_value(test_node, 'test_params'),
        'severity': elementary.insensitive_get_dict_value(test_node, 'severity'),
        'status': elementary.insensitive_get_dict_value(run_result_dict, 'status')
    } %}
    {{ return(alert_dict) }}
{% endmacro %}

{% macro convert_schema_change_dict_to_alert(run_result_dict, schema_change_dict, test_node) %}
    {% set alert_dict = schema_change_dict %}
    {% do alert_dict.update({
        'model_unique_id': elementary.insensitive_get_dict_value(test_node, 'parent_model_unique_id'),
        'owners': elementary.insensitive_get_dict_value(test_node, 'model_owners'),
        'tags': elementary.insensitive_get_dict_value(test_node, 'model_tags'),
        'alert_results_query': elementary.insensitive_get_dict_value(test_node, 'compiled_sql'),
        'test_name': elementary.insensitive_get_dict_value(test_node, 'short_name'),
        'test_params': elementary.insensitive_get_dict_value(test_node, 'test_params'),
        'severity': elementary.insensitive_get_dict_value(test_node, 'severity'),
        'status': elementary.insensitive_get_dict_value(run_result_dict, 'status')
    }) %}
    {{ return(alert_dict) }}
{% endmacro %}

{% macro convert_dbt_test_to_alert(run_result_dict, test_node) %}
    {% set test_execution_id = elementary.get_node_execution_id(test_node) %}
    {% set parent_model_unique_id = elementary.insensitive_get_dict_value(test_node, 'parent_model_unique_id') %}
    {% set parent_model = elementary.get_node(parent_model_unique_id) %}
    {% set parent_model_name = elementary.get_table_name_from_node(parent_model) %}
    {% set alert_dict = {
        'alert_id': test_execution_id,
        'data_issue_id': none,
        'test_execution_id': test_execution_id,
        'test_unique_id': elementary.insensitive_get_dict_value(test_node, 'unique_id'),
        'model_unique_id': elementary.insensitive_get_dict_value(test_node, 'parent_model_unique_id'),
        'detected_at': run_started_at.strftime('%Y-%m-%d %H:%M:%S'),
        'database_name': elementary.insensitive_get_dict_value(test_node, 'database_name'),
        'schema_name': elementary.insensitive_get_dict_value(test_node, 'schema_name'),
        'table_name': parent_model_name,
        'column_name': elementary.insensitive_get_dict_value(test_node, 'test_column_name'),
        'alert_type': 'dbt_test',
        'sub_type': elementary.insensitive_get_dict_value(test_node, 'short_name'),
        'alert_description': elementary.insensitive_get_dict_value(run_result_dict, 'message'),
        'other': none,
        'owners': elementary.insensitive_get_dict_value(test_node, 'model_owners'),
        'tags': elementary.insensitive_get_dict_value(test_node, 'model_tags'),
        'alert_results_query': elementary.insensitive_get_dict_value(test_node, 'compiled_sql'),
        'test_name': elementary.insensitive_get_dict_value(test_node, 'short_name'),
        'test_params': elementary.insensitive_get_dict_value(test_node, 'test_params'),
        'severity': elementary.insensitive_get_dict_value(test_node, 'severity'),
        'status': elementary.insensitive_get_dict_value(run_result_dict, 'status')
    }%}
    {{ return(alert_dict) }}
{% endmacro %}

{% macro get_test_result_rows_as_dicts(flatten_test_node, sample_limit = none) %}
    {% set test_row_dicts = [] %}
    {% set test_compiled_sql = flatten_test_node.compiled_sql %}
    {% if test_compiled_sql %}
        {% if sample_limit %}
            {% set test_compiled_sql = test_compiled_sql ~ ' limit ' ~ sample_limit %}
        {% endif %}
        {% set test_table_agate = run_query(test_compiled_sql) %}
        {% set test_row_dicts = elementary.agate_to_dicts(test_table_agate) %}
    {% endif %}
    {{ return(test_row_dicts) }}
{% endmacro %}

{% macro get_test_metrics_table(database_name, schema_name, test_node) %}
    {% set temp_metrics_table_name = test_node.name ~ '__metrics' %}
    {% set tests_schema_name = schema_name ~ '__tests' %}
    {% set temp_metrics_table_relation = adapter.get_relation(database=database_name,
                                                              schema=tests_schema_name,
                                                              identifier=temp_metrics_table_name) %}
    {% if temp_metrics_table_relation %}
        {% set full_metrics_table_name = temp_metrics_table_relation.render() %}
        {{ return(full_metrics_table_name) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}

{% macro get_data_monitoring_metrics_relation(database_name, schema_name) %}
    {%- set data_monitoring_metrics_relation = adapter.get_relation(database=database_name,
                                                                    schema=schema_name,
                                                                    identifier='data_monitoring_metrics') -%}
    {{ return(data_monitoring_metrics_relation) }}
{% endmacro %}

{% macro merge_data_monitoring_metrics(database_name, schema_name, test_metrics_tables) %}
    {%- if test_metrics_tables %}
        {%- set test_tables_union_query = elementary.union_metrics_query(test_metrics_tables) -%}
        {%- set target_relation = elementary.get_data_monitoring_metrics_relation(database_name, schema_name) -%}
        {%- set temp_relation = dbt.make_temp_relation(target_relation) -%}
        {%- if test_tables_union_query %}
            {{ elementary.debug_log('Running union query from test tables to ' ~ temp_relation.identifier) }}
            {%- do run_query(dbt.create_table_as(True, temp_relation, test_tables_union_query)) %}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
            {{ elementary.debug_log('Merging ' ~ temp_relation.identifier ~ ' to ' ~ target_relation.database ~ '.' ~ target_relation.schema ~ '.' ~ target_relation.identifier) }}
            {%- if target_relation and temp_relation and dest_columns %}
                {% set merge_sql = elementary.merge_sql(target_relation, temp_relation, 'id', dest_columns) %}
                {%- do run_query(merge_sql) %}
                {%- do adapter.commit() -%}
                {{ elementary.debug_log('Finished merging') }}
            {%- else %}
                {{ elementary.debug_log('Error: could not merge to table: ' ~ target_name) }}
            {%- endif %}
        {%- endif %}
    {%- endif %}
{% endmacro %}



