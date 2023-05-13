{% test table_anomalies(model, table_anomalies, timestamp_column, where_expression, anomaly_sensitivity, anomaly_direction, min_training_set_size, time_bucket, days_back, backfill_days, seasonality, mandatory_params=none, event_timestamp_column=none, freshness_column=none, sensitivity=none) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('alerts_anomaly_detection') }}
    -- depends_on: {{ ref('metrics_anomaly_score') }}
    -- depends_on: {{ ref('dbt_run_results') }}

    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {%- if elementary.is_ephemeral_model(model) %}
            {{ exceptions.raise_compiler_error("The test is not supported for ephemeral models, model name: {}".format(model.identifier)) }}
        {%- endif %}

        {% set test_table_name = elementary.get_elementary_test_table_name() %}
        {{ elementary.debug_log('collecting metrics for test: ' ~ test_table_name) }}
        {#- creates temp relation for test metrics -#}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}

        {#- get table configuration -#}
        {%- set full_table_name = elementary.relation_to_full_name(model) %}
        {%- set model_relation = dbt.load_relation(model) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("Unable to find table `{}`".format(full_table_name)) }}
        {% endif %}

        {% set test_configuration, metric_properties = elementary.get_anomalies_test_configuration(model_relation=model_relation,
                                                                                                   mandatory_params=mandatory_params,
                                                                                                   timestamp_column=timestamp_column,
                                                                                                   where_expression=where_expression,
                                                                                                   anomaly_sensitivity=anomaly_sensitivity,
                                                                                                   anomaly_direction=anomaly_direction,
                                                                                                   min_training_set_size=min_training_set_size,
                                                                                                   time_bucket=time_bucket,
                                                                                                   days_back=days_back,
                                                                                                   backfill_days=backfill_days,
                                                                                                   seasonality=seasonality,
                                                                                                   event_timestamp_column=event_timestamp_column,
                                                                                                   sensitivity=sensitivity) %}
        {% if not test_configuration %}
            {{ exceptions.raise_compiler_error("Failed to create test configuration dict for test `{}`".format(test_table_name)) }}
        {% endif %}
        {{ elementary.debug_log('test configuration - ' ~ test_configuration) }}
        {%- set table_monitors = elementary.get_final_table_monitors(table_anomalies) %}
        {{ elementary.debug_log('table_monitors - ' ~ table_monitors) }}
        {% if test_configuration.timestamp_column %}
            {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model,
                                                                                            test_configuration.backfill_days,
                                                                                            test_configuration.days_back,
                                                                                            monitors=table_monitors,
                                                                                            metric_properties=metric_properties) %}
        {%- endif %}
        {{ elementary.debug_log('min_bucket_start: ' ~ min_bucket_start ~ ' | max_bucket_end: ' ~ min_bucket_start ) }}

        {#- execute table monitors and write to temp test table -#}
        {{ elementary.test_log('start', full_table_name) }}
        {%- set table_monitoring_query = elementary.table_monitoring_query(model_relation,
                                                                           min_bucket_start,
                                                                           max_bucket_end,
                                                                           table_monitors,
                                                                           test_configuration.days_back,
                                                                           metric_properties=metric_properties) %}
        {{ elementary.debug_log('table_monitoring_query - \n' ~ table_monitoring_query) }}
        {% set temp_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'metrics', table_monitoring_query) %}

        {#- calculate anomaly scores for metrics -#}
        {% set anomaly_scores_query = elementary.get_anomaly_scores_query(temp_table_relation,
                                                                          model_relation,
                                                                          test_configuration=test_configuration,
                                                                          metric_properties=metric_properties) %}
        {{ elementary.debug_log('table monitors anomaly scores query - \n' ~ anomaly_scores_query) }}
        
        {% set anomaly_scores_test_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'anomaly_scores', anomaly_scores_query) %}
        {{ elementary.test_log('end', full_table_name) }}
        {{ elementary.get_read_anomaly_scores_query() }}
    {% else %}

        {# test must run an sql query #}
        {{ elementary.no_results_query() }}

    {% endif %}

{% endtest %}


