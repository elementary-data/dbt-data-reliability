{% test column_anomalies(model, column_name, column_anomalies, timestamp_column=none, sensitivity=none, backfill_days=none) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('alerts_anomaly_detection') }}
    -- depends_on: {{ ref('metrics_anomaly_score') }}
    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {% set test_name_in_graph = elementary.get_test_name_in_graph() %}
        {{ elementary.debug_log('collecting metrics for test: ' ~ test_name_in_graph) }}
        {#- creates temp relation for test metrics -#}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}

        {#- get column configuration -#}
        {%- set full_table_name = elementary.relation_to_full_name(model) %}
        {%- set model_relation = dbt.load_relation(model) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("Unable to find table `{}`".format(full_table_name)) }}
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
            {{ exceptions.raise_compiler_error("Unable to find column `{}` in `{}`".format(column_name, full_table_name)) }}
        {%- endif -%}
        {%- set column_monitors = column_obj_and_monitors['monitors'] -%}
        {%- set column_obj = column_obj_and_monitors['column'] -%}
        {{ elementary.debug_log('column_monitors - ' ~ column_monitors) }}
        {% set backfill_days = elementary.get_test_argument(argument_name='backfill_days', value=backfill_days) %}
        {%- set min_bucket_start = "'" ~ elementary.get_min_bucket_start(full_table_name, backfill_days, column_monitors, column_name) ~ "'" %}
        {{ elementary.debug_log('min_bucket_start - ' ~ min_bucket_start) }}
        {#- execute table monitors and write to temp test table -#}
        {{ elementary.test_log('start', full_table_name, column_name) }}
        {%- set column_monitoring_query = elementary.column_monitoring_query(model_relation, timestamp_column, is_timestamp, min_bucket_start, column_obj, column_monitors) %}
        {{ elementary.debug_log('column_monitoring_query - \n' ~ column_monitoring_query) }}
        {% set temp_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_name_in_graph, 'metrics', column_monitoring_query) %}

        {#- calculate anomaly scores for metrics -#}
        {%- set temp_table_name = elementary.relation_to_full_name(temp_table_relation) %}
        {%- set sensitivity = elementary.get_test_argument(argument_name='anomaly_sensitivity', value=sensitivity) %}
        {% set anomaly_scores_query = elementary.get_anomaly_scores_query(temp_table_relation, full_table_name, sensitivity, backfill_days, column_monitors, column_name) %}
        {{ elementary.debug_log('anomaly_score_query - \n' ~ anomaly_scores_query) }}
        {% set anomaly_scores_test_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_name_in_graph, 'anomaly_scores', anomaly_scores_query) %}
        {{ elementary.test_log('end', full_table_name, column_name) }}

        {{ elementary.get_read_anomaly_scores_query() }}

    {%- else %}

        {#- test must run an sql query -#}
        {{ elementary.no_results_query() }}

    {%- endif %}
{% endtest %}



