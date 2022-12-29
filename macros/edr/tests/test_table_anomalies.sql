{% test table_anomalies(model, table_anomalies, freshness_column, timestamp_column, sensitivity, backfill_days, where_expression, time_bucket) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('alerts_anomaly_detection') }}
    -- depends_on: {{ ref('metrics_anomaly_score') }}
    -- depends_on: {{ ref('dbt_run_results') }}
    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {% if not time_bucket %}
          {% set time_bucket = elementary.get_default_time_bucket() %}
        {% endif %}

        {% set test_name_in_graph = elementary.get_test_name_in_graph() %}
        {{ elementary.debug_log('collecting metrics for test: ' ~ test_name_in_graph) }}
        {#- creates temp relation for test metrics -#}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}

        {#- get table configuration -#}
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

        {% if timestamp_column and not is_timestamp %}
          {% do exceptions.raise_compiler_error("Column `{}` is not a timestamp.".format(timestamp_column)) %}
        {% endif %}

        {%- set table_monitors = elementary.get_final_table_monitors(table_anomalies) %}
        {{ elementary.debug_log('table_monitors - ' ~ table_monitors) }}
        {% set backfill_days = elementary.get_test_argument(argument_name='backfill_days', value=backfill_days) %}
        {%- set min_bucket_start = elementary.quote(elementary.get_test_min_bucket_start(model_graph_node, backfill_days, table_monitors)) %}
        {{ elementary.debug_log('min_bucket_start - ' ~ min_bucket_start) }}
        {#- execute table monitors and write to temp test table -#}
        {{ elementary.test_log('start', full_table_name) }}
        {%- set table_monitoring_query = elementary.table_monitoring_query(model_relation, timestamp_column, min_bucket_start, table_monitors, freshness_column, where_expression, time_bucket) %}
        {{ elementary.debug_log('table_monitoring_query - \n' ~ table_monitoring_query) }}
        {% set temp_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_name_in_graph, 'metrics', table_monitoring_query) %}

        {#- calculate anomaly scores for metrics -#}
        {%- set sensitivity = elementary.get_test_argument(argument_name='anomaly_sensitivity', value=sensitivity) %}
        {% set anomaly_scores_query = elementary.get_anomaly_scores_query(temp_table_relation, model_graph_node, sensitivity, backfill_days, table_monitors) %}
        {{ elementary.debug_log('table monitors anomaly scores query - \n' ~ anomaly_scores_query) }}
        {% set anomaly_scores_test_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_name_in_graph, 'anomaly_scores', anomaly_scores_query) %}
        {{ elementary.test_log('end', full_table_name) }}

        {{ elementary.get_read_anomaly_scores_query() }}

    {% else %}

        {# test must run an sql query #}
        {{ elementary.no_results_query() }}

    {% endif %}

{% endtest %}


