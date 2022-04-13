{% test all_columns_anomalies(model, column_anomalies = none) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('alerts_data_monitoring') }}
    -- depends_on: {{ ref('metrics_anomaly_score') }}
    {%- if execute %}
        {% set test_name_in_graph = elementary.get_test_name_in_graph() %}
        {{ elementary.debug_log('collecting metrics for test: ' ~ test_name_in_graph) }}
        {#- creates temp relation for test metrics -#}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set schema_name = schema_name ~ '__tests' %}
        {%- set temp_metrics_table_name = test_name_in_graph ~ '__metrics' %}
        {{ elementary.debug_log('metrics table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_metrics_table_name) }}
        {%- set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_metrics_table_name,
                                                                                   type='table') -%}
        {%- if not adapter.check_schema_exists(database_name, schema_name) %}
            {{ elementary.debug_log('schema ' ~ database_name ~ '.' ~ schema_name ~ ' doesnt exist, creating it') }}
            {%- do dbt.create_schema(temp_table_relation) %}
        {%- endif %}

        {#- get column configuration -#}
        {%- set table_config = elementary.get_table_config_from_graph(model) %}
        {{ elementary.debug_log('table config - ' ~ table_config) }}
        {%- set full_table_name = elementary.relation_to_full_name(model) %}
        {%- set model_relation = dbt.load_relation(model) %}
        {% if not model_relation %}
            {{ elementary.test_log('monitored_table_not_found', full_table_name) }}
            {{ return(elementary.no_results_query()) }}
        {% endif %}

        {%- set empty_table_query = elementary.empty_data_monitoring_metrics() %}
        {%- set timestamp_column = elementary.insensitive_get_dict_value(table_config, 'timestamp_column') %}
        {{ elementary.debug_log('timestamp_column - ' ~ timestamp_column) }}
        {%- set timestamp_column_data_type = elementary.insensitive_get_dict_value(table_config, 'timestamp_column_data_type') %}
        {{ elementary.debug_log('timestamp_column_data_type - ' ~ timestamp_column_data_type) }}
        {%- set is_timestamp = elementary.get_is_column_timestamp(model_relation, timestamp_column, timestamp_column_data_type) %}
        {{ elementary.debug_log('is_timestamp - ' ~ is_timestamp) }}
        {%- set min_bucket_start = "'" ~ elementary.get_min_bucket_start(full_table_name, column_tests) ~ "'" %}
        {{ elementary.debug_log('min_bucket_start - ' ~ min_bucket_start) }}
        {%- set columns_config = elementary.get_all_columns_monitors(model_relation, column_anomalies) -%}
        {{ elementary.debug_log('columns_config - ' ~ columns_config) }}

        {#- execute table monitors and write to temp test table -#}
        {%- if columns_config | length > 0 %}
            {{ elementary.test_log('start', full_table_name, 'all columns') }}
            {%- for column in columns_config %}
                {%- set column_name = column['column_name'] %}
                {%- set column_monitors = column['monitors'] %}
                {%- set column_monitoring_query = elementary.column_monitoring_query(model_relation, timestamp_column, is_timestamp, min_bucket_start, column_name, column_monitors) %}
                {%- if loop.first %}
                    {%- do dbt.drop_relation_if_exists(temp_table_relation) %}
                    {%- do run_query(dbt.create_table_as(False, temp_table_relation, empty_table_query)) %}
                {% endif %}
                {%- do run_query(elementary.insert_as_select(temp_table_relation, column_monitoring_query)) -%}
            {%- endfor %}
        {%- endif %}

        {#- query if there is an anomaly in recent metrics -#}
        {% set anomaly_query = elementary.get_anomaly_query(temp_table_relation, full_table_name, columns_only=true) %}
        {% set temp_alerts_table_name = test_name_in_graph ~ '__anomalies' %}
        {{ elementary.debug_log('anomalies table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_alerts_table_name) }}
        {% set anomalies_temp_table_exists, anomalies_temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_alerts_table_name,
                                                                                   type='table') -%}
        {%- do dbt.drop_relation_if_exists(anomalies_temp_table_relation) %}
        {% do run_query(dbt.create_table_as(False, anomalies_temp_table_relation, anomaly_query)) %}

        {{ elementary.test_log('end', full_table_name, 'all columns') }}

        {# return anomalies query as standard test query #}
        select * from {{ anomalies_temp_table_relation }}

    {%- else %}

        {{ elementary.test_log('no_monitors', full_table_name, 'all columns') }}
        {#- test must run an sql query -#}
        {{ elementary.no_results_query() }}

    {%- endif %}
{% endtest %}


