{% test column_anomalies(model, column_name, column_anomalies, timestamp_column=none, sensitivity=none, backfill_days=none, period=none) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('alerts_anomaly_detection') }}
    -- depends_on: {{ ref('metrics_anomaly_score') }}
    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {% set test_name_in_graph = elementary.get_test_name_in_graph() %}
        {{ elementary.debug_log('collecting metrics for test: ' ~ test_name_in_graph) }}
        {#- creates temp relation for test metrics -#}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set schema_name = schema_name ~ elementary.get_config_var('tests_schema_name') %}
        {%- set temp_metrics_table_name = elementary.table_name_with_suffix(test_name_in_graph, '__metrics') %}
        {{ elementary.debug_log('metrics table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_metrics_table_name) }}
        {%- set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_metrics_table_name,
                                                                                   type='table') -%}

        {#- get column configuration -#}
        {%- set full_table_name = elementary.relation_to_full_name(model) %}
        {%- set model_relation = dbt.load_relation(model) %}
        {% if not model_relation %}
            {{ elementary.test_log('monitored_table_not_found', full_table_name) }}
            {{ return(elementary.no_results_query()) }}
        {% endif %}

        {% set model_graph_node = elementary.get_model_graph_node(model_relation) %}
        {% set timestamp_column = elementary.get_timestamp_column(timestamp_column, model_graph_node) %}

        {%- set timestamp_column_data_type = elementary.find_normalized_data_type_for_column(model, timestamp_column) %}
        {{ elementary.debug_log('timestamp_column - ' ~ timestamp_column) }}
        {{ elementary.debug_log('timestamp_column_data_type - ' ~ timestamp_column_data_type) }}
        {%- set is_timestamp = elementary.get_is_column_timestamp(model_relation, timestamp_column, timestamp_column_data_type) %}
        {{ elementary.debug_log('is_timestamp - ' ~ is_timestamp) }}

        {% set period = elementary.get_period(period, model_graph_node) %}
        {{ elementary.debug_log('period - ' ~ period) }}
        
        {%- set column_obj_and_monitors = elementary.get_column_obj_and_monitors(model, column_name, column_anomalies) -%}
        {%- if not column_obj_and_monitors -%}
            {{ elementary.edr_log('column ' ~ column_name ~ ' object was not found') }}
            {{ return(elementary.no_results_query()) }}
        {%- endif -%}
        {%- set column_monitors = column_obj_and_monitors['monitors'] -%}
        {%- set column_obj = column_obj_and_monitors['column'] -%}
        {{ elementary.debug_log('column_monitors - ' ~ column_monitors) }}
        {% set backfill_days = elementary.get_test_argument(argument_name='backfill_days', value=backfill_days) %}
        {%- set min_bucket_start = "'" ~ elementary.get_min_bucket_start(full_table_name, backfill_days, period, column_monitors, column_name) ~ "'" %}
        {{ elementary.debug_log('min_bucket_start - ' ~ min_bucket_start) }}
        
        {#- execute table monitors and write to temp test table -#}
        {{ elementary.test_log('start', full_table_name, column_name) }}
        {%- set column_monitoring_query = elementary.column_monitoring_query(model_relation, timestamp_column, is_timestamp, min_bucket_start, column_obj, column_monitors, period) %}
        {{ elementary.debug_log('column_monitoring_query - \n' ~ column_monitoring_query) }}
        {%- do elementary.create_or_replace(False, temp_table_relation, column_monitoring_query) %}

        {#- calculate anomaly scores for metrics -#}
        {%- set temp_table_name = elementary.relation_to_full_name(temp_table_relation) %}
        {%- set sensitivity = elementary.get_test_argument(argument_name='anomaly_sensitivity', value=sensitivity) %}
        {% set anomaly_scores_query = elementary.get_anomaly_scores_query(temp_table_relation, full_table_name, sensitivity, backfill_days, column_monitors, period, column_name) %}
        {{ elementary.debug_log('anomaly_score_query - \n' ~ anomaly_scores_query) }}
        {%- set anomaly_scores_test_table_name = elementary.table_name_with_suffix(test_name_in_graph, '__anomaly_scores') %}
        {{ elementary.debug_log('anomalies table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ anomaly_scores_test_table_name) }}
        {% set anomaly_scores_test_table_exists, anomaly_scores_test_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=anomaly_scores_test_table_name,
                                                                                   type='table') -%}
        {%- do elementary.create_or_replace(False, anomaly_scores_test_table_relation, anomaly_scores_query) %}
        {{ elementary.test_log('end', full_table_name, column_name) }}

        {# return anomalies query as standart test query #}
        {{ elementary.get_anomaly_query(anomaly_scores_test_table_relation, sensitivity, backfill_days, period) }}
    
    {%- else %}

        {#- test must run an sql query -#}
        {{ elementary.no_results_query() }}

    {%- endif %}
{% endtest %}



