{% test column_anomalies(model, column_name, column_anomalies, timestamp_column=none, sensitivity=none, backfill_days=none) %}
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

        {%- set column_obj_and_monitors = elementary.get_column_obj_and_monitors(model, column_name, column_anomalies) -%}
        {%- if not column_obj_and_monitors -%}
            {{ elementary.edr_log('column ' ~ column_name ~ ' object was not found') }}
            {{ return(elementary.no_results_query()) }}
        {%- endif -%}
        {%- set column_monitors = column_obj_and_monitors['monitors'] -%}
        {%- set column_obj = column_obj_and_monitors['column'] -%}
        {{ elementary.debug_log('column_monitors - ' ~ column_monitors) }}
        {%- set min_bucket_start = "'" ~ elementary.get_min_bucket_start(full_table_name, column_monitors, column_name, backfill_days) ~ "'" %}
        {{ elementary.debug_log('min_bucket_start - ' ~ min_bucket_start) }}
        {#- execute table monitors and write to temp test table -#}
        {{ elementary.test_log('start', full_table_name, column_name) }}
        {%- set column_monitoring_query = elementary.column_monitoring_query(model_relation, timestamp_column, is_timestamp, min_bucket_start, column_obj, column_monitors) %}
        {{ elementary.debug_log('column_monitoring_query - \n' ~ column_monitoring_query) }}
        {%- do elementary.create_or_replace(False, temp_table_relation, column_monitoring_query) %}

        {#- calculate anomaly scores for metrics -#}
        {%- set temp_table_name = elementary.relation_to_full_name(temp_table_relation) %}
        {%- set anomaly_sensitivity = elementary.get_anomaly_sensitivity(sensitivity) %}
        {% set anomaly_scores_query = elementary.get_anomaly_scores_query(temp_table_relation, full_table_name, column_monitors, column_name, sensitivity=anomaly_sensitivity, backfill_days=backfill_days) %}
        {{ elementary.debug_log('anomaly_query - \n' ~ anomaly_query) }}
        {%- set anomaly_scores_test_table_name = elementary.table_name_with_suffix(test_name_in_graph, '__anomalies') %}
        {{ elementary.debug_log('anomalies table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ anomaly_scores_test_table_name) }}
        {% set anomaly_scores_test_table_exists, anomaly_scores_test_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=anomaly_scores_test_table_name,
                                                                                   type='table') -%}
        {%- do elementary.create_or_replace(False, anomaly_scores_test_table_relation, anomaly_scores_query) %}
        {{ elementary.test_log('end', full_table_name, column_name) }}

        {# return anomalies query as standart test query #}
        {{ elementary.get_anomaly_query(anomaly_scores_test_table_relation, anomaly_sensitivity) }}
    
    {%- else %}

        {#- test must run an sql query -#}
        {{ elementary.test_log('no_monitors', full_table_name, column_name) }}
        {{ elementary.no_results_query() }}

    {%- endif %}
{% endtest %}



