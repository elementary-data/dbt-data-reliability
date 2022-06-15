{% macro handle_test_results(results) %}
    {% if execute and flags.WHICH in ['test', 'build'] %}
        {% set test_metrics_tables = [] %}
        {% set elementary_test_results = [] %}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% for result in results | selectattr('node.resource_type', '==', 'test') %}
            {% set status = result.status | lower %}
            {% set run_result_dict = result.to_dict() %}
            {% set test_node = elementary.safe_get_with_default(run_result_dict, 'node', {}) %}
            {% set flatten_test_node = elementary.flatten_test(test_node) %}
            {% if flatten_test_node.test_namespace == 'elementary' %}
                {% if flatten_test_node.short_name in ['table_anomalies', 'column_anomalies', 'all_columns_anomalies'] %}
                    {% set test_metrics_table = elementary.get_elementary_test_table(database_name, schema_name, flatten_test_node.name, '__metrics') %}
                    {% if test_metrics_table %}
                        {% do test_metrics_tables.append(test_metrics_table) %}
                    {% endif %}
                    {%- if status == 'error' -%}
                        {% do elementary_test_results.append(elementary.get_dbt_test_result(run_result_dict,
                                                                                            flatten_test_node,
                                                                                            'anomaly_detection')) %}
                    {%- else -%}
                        {% do elementary_test_results.extend(elementary.get_test_result_per_metric(database_name,
                                                                                                   schema_name,
                                                                                                   status,
                                                                                                   run_result_dict,
                                                                                                   flatten_test_node)) %}
                    {%- endif -%}
                {% elif flatten_test_node.short_name == 'schema_changes' %}
                    {% if status == 'error' or status == 'pass' %}
                        {% do elementary_test_results.append(elementary.get_dbt_test_result(run_result_dict,
                                                                                            flatten_test_node,
                                                                                            'schema_change')) %}
                    {%- else -%} {# warn or fail #}
                        {% do elementary_test_results.extend(elementary.get_test_result_per_schmea_change(database_name,
                                                                                                          schema_name,
                                                                                                          run_result_dict,
                                                                                                          flatten_test_node)) %}
                    {%- endif -%}
                {% endif %}
            {% else %}
                {% do elementary_test_results.append(elementary.get_dbt_test_result(run_result_dict,
                                                                                    flatten_test_node,
                                                                                    'dbt_test')) %}
            {% endif %}
        {% endfor %}
        {{ elementary.merge_data_monitoring_metrics(database_name, schema_name, test_metrics_tables) }}
        {% if elementary_test_results %}
            {%- set elementary_test_results_relation = adapter.get_relation(database=database_name,
                                                                            schema=schema_name,
                                                                            identifier='elementary_test_results') -%}
            {%- do elementary.insert_dicts(elementary_test_results_relation, elementary_test_results) -%}
        {% endif %}
    {% endif %}
    {{ return('') }}
{% endmacro %}

{%- macro get_test_result_per_metric(database_name, schema_name, status, run_result_dict, flatten_test_node) -%}
    {% set anomaly_detection_test_results = [] %}
    {% set test_anomaly_scores_table = elementary.get_elementary_test_table(database_name, schema_name, flatten_test_node.name, '__anomalies') %}
    {%- if status != 'pass' -%} {# warn or fail #}
        {% set test_row_dicts = elementary.get_test_result_rows_as_dicts(flatten_test_node) %}
    {% else %}
        {% set test_row_dicts = elementary.get_most_recent_anomaly_scores(test_anomaly_scores_table) %}
    {% endif %}
    {% for test_row_dict in test_row_dicts %}
        {% do anomaly_detection_test_results.append(elementary.get_metric_test_result(run_result_dict,
                                                                                      test_row_dict,
                                                                                      flatten_test_node,
                                                                                      test_anomaly_scores_table)) %}
    {% endfor %}
    {{- return(anomaly_detection_test_results) -}}
{%- endmacro -%}

{%- macro get_test_result_per_schmea_change(database_name, schema_name, run_result_dict, flatten_test_node) -%}
    {% set schema_change_test_results = [] %}
    {% set test_row_dicts = elementary.get_test_result_rows_as_dicts(flatten_test_node) %}
    {% for test_row_dict in test_row_dicts %}
        {% do schema_change_test_results.append(elementary.get_schema_change_test_result(run_result_dict,
                                                                                         test_row_dict,
                                                                                         flatten_test_node)) %}
    {% endfor %}
    {{- return(schema_change_test_results) -}}
{%- endmacro -%}

{% macro get_metric_test_result(run_result_dict, anomaly_dict, test_node, test_anomaly_scores_table) %}
    {% set full_table_name = elementary.insensitive_get_dict_value(anomaly_dict, 'full_table_name', '') %}
    {% set split_full_table_name = full_table_name.split('.') %}
    {% set database_name = split_full_table_name[0] %}
    {% set schema_name = split_full_table_name[1] %}
    {% set table_name = split_full_table_name[2] %}
    {% set test_params = elementary.insensitive_get_dict_value(test_node, 'test_params', {}) %}
    {% set test_param_sensitivity = elementary.insensitive_get_dict_value(test_params, 'sensitivity') %}
    {% set test_param_timestamp_column = elementary.insensitive_get_dict_value(test_params, 'timestamp_column') %}
    {% set parent_model_unique_id = elementary.insensitive_get_dict_value(test_node, 'parent_model_unique_id') %}
    {% set parent_model_node = elementary.get_node(parent_model_unique_id) %}
    {% set timestamp_column = elementary.get_timestamp_column(test_param_timestamp_column, parent_model_node) %}
    {% set sensitivity = elementary.get_anomaly_sensitivity(test_param_sensitivity) %}
    {% do test_params.update({'sensitivity': sensitivity}) %}
    {% do test_params.update({'timestamp_column': timestamp_column}) %}
    {% set column_name = elementary.insensitive_get_dict_value(anomaly_dict, 'column_name') %}
    {% set metric_name = elementary.insensitive_get_dict_value(anomaly_dict, 'metric_name') %}
    {% set metric_id = elementary.insensitive_get_dict_value(anomaly_dict, 'metric_id') %}
    {% set test_results_query %}
        select min_metric_value as min_value,
               max_metric_value as max_value,
               metric_value as value,
               bucket_start as start_time,
               bucket_end as end_time,
               metric_id
        from {{ test_anomaly_scores_table }}
        where upper(full_table_name) = upper({{ elementary.const_as_string(full_table_name) }})
              and metric_name = {{ elementary.const_as_string(metric_name) }}
              {%- if column_name %}
                and upper(column_name) = upper({{ elementary.const_as_string(column_name) }})
              {%- endif %}
        order by bucket_end desc
    {%- endset -%}
    {% set test_result_dict = {
        'id': elementary.insensitive_get_dict_value(anomaly_dict, 'id'),
        'data_issue_id': elementary.insensitive_get_dict_value(anomaly_dict, 'metric_id'),
        'test_execution_id': elementary.insensitive_get_dict_value(anomaly_dict, 'test_execution_id'),
        'test_unique_id': elementary.insensitive_get_dict_value(anomaly_dict, 'test_unique_id'),
        'model_unique_id': elementary.insensitive_get_dict_value(test_node, 'parent_model_unique_id'),
        'detected_at': elementary.insensitive_get_dict_value(anomaly_dict, 'detected_at'),
        'database_name': database_name,
        'schema_name': schema_name,
        'table_name': table_name,
        'column_name': column_name,
        'test_type': 'anomaly_detection',
        'test_sub_type': metric_name,
        'test_results_description': elementary.insensitive_get_dict_value(anomaly_dict, 'anomaly_description'),
        'other': elementary.insensitive_get_dict_value(anomaly_dict, 'anomalous_value'),
        'owners': elementary.insensitive_get_dict_value(test_node, 'model_owners'),
        'tags': elementary.insensitive_get_dict_value(test_node, 'model_tags'),
        'test_results_query': test_results_query,
        'test_name': elementary.insensitive_get_dict_value(test_node, 'short_name'),
        'test_params': elementary.insensitive_get_dict_value(test_node, 'test_params'),
        'severity': elementary.insensitive_get_dict_value(test_node, 'severity'),
        'status': elementary.insensitive_get_dict_value(run_result_dict, 'status')
    } %}
    {{ return(test_result_dict) }}
{% endmacro %}

{% macro get_schema_change_test_result(run_result_dict, schema_change_dict, test_node) %}
    {% set test_result_dict = schema_change_dict %}
    {% do test_result_dict.update({
        'other': none,
        'model_unique_id': elementary.insensitive_get_dict_value(test_node, 'parent_model_unique_id'),
        'owners': elementary.insensitive_get_dict_value(test_node, 'model_owners'),
        'tags': elementary.insensitive_get_dict_value(test_node, 'model_tags'),
        'test_results_query': elementary.insensitive_get_dict_value(test_node, 'compiled_sql'),
        'test_name': elementary.insensitive_get_dict_value(test_node, 'short_name'),
        'test_params': elementary.insensitive_get_dict_value(test_node, 'test_params'),
        'severity': elementary.insensitive_get_dict_value(test_node, 'severity'),
        'status': elementary.insensitive_get_dict_value(run_result_dict, 'status')
    }) %}
    {{ return(test_result_dict) }}
{% endmacro %}

{% macro get_dbt_test_result(run_result_dict, test_node, test_type) %}
    {% set test_execution_id = elementary.get_node_execution_id(test_node) %}
    {% set parent_model_unique_id = elementary.insensitive_get_dict_value(test_node, 'parent_model_unique_id') %}
    {% set parent_model = elementary.get_node(parent_model_unique_id) %}
    {% set parent_model_name = elementary.get_table_name_from_node(parent_model) %}
    {% set test_result_dict = {
        'id': test_execution_id,
        'data_issue_id': none,
        'test_execution_id': test_execution_id,
        'test_unique_id': elementary.insensitive_get_dict_value(test_node, 'unique_id'),
        'model_unique_id': elementary.insensitive_get_dict_value(test_node, 'parent_model_unique_id'),
        'detected_at': run_started_at.strftime('%Y-%m-%d %H:%M:%S'),
        'database_name': elementary.insensitive_get_dict_value(test_node, 'database_name'),
        'schema_name': elementary.insensitive_get_dict_value(test_node, 'schema_name'),
        'table_name': parent_model_name,
        'column_name': elementary.insensitive_get_dict_value(test_node, 'test_column_name'),
        'test_type': test_type,
        'test_sub_type': elementary.insensitive_get_dict_value(test_node, 'short_name'),
        'test_results_description': elementary.insensitive_get_dict_value(run_result_dict, 'message'),
        'other': none,
        'owners': elementary.insensitive_get_dict_value(test_node, 'model_owners'),
        'tags': elementary.insensitive_get_dict_value(test_node, 'model_tags'),
        'test_results_query': elementary.insensitive_get_dict_value(test_node, 'compiled_sql'),
        'test_name': elementary.insensitive_get_dict_value(test_node, 'short_name'),
        'test_params': elementary.insensitive_get_dict_value(test_node, 'test_params'),
        'severity': elementary.insensitive_get_dict_value(test_node, 'severity'),
        'status': elementary.insensitive_get_dict_value(run_result_dict, 'status')
    }%}
    {{ return(test_result_dict) }}
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

{% macro get_most_recent_anomaly_scores(test_anomaly_scores_table) %}
    {% set test_metrics_dicts = [] %}
    {% set most_recent_anomaly_scores_query %}
        with anomaly_scores as (
            select *,
                row_number() over (partition by full_table_name, column_name, metric_name order by bucket_end desc) as row_number
            from {{ test_anomaly_scores_table }}
        ),
        most_recent_scores as (
            select * from anomaly_scores where row_number = 1
        )
        select * from most_recent_scores
    {% endset %}
    {% set most_recent_anomaly_scores_agate = run_query(most_recent_anomaly_scores_query) %}
    {% set most_recent_anomaly_scores = elementary.agate_to_dicts(most_recent_anomaly_scores_agate) %}
    {{ return(most_recent_anomaly_scores) }}
{% endmacro %}

{% macro get_elementary_test_table(database_name, schema_name, test_name, suffix) %}
    {% set tests_schema_name = schema_name ~ '__tests' %}
    {% set test_table_name = elementary.table_name_with_suffix(test_name, suffix) %}
    {% set test_table_relation = adapter.get_relation(database=database_name,
                                                      schema=tests_schema_name,
                                                      identifier=test_table_name) %}
    {{ return(test_table_relation) }}
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



