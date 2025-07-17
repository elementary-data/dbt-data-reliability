{% test table_anomalies(model, table_anomalies, timestamp_column, where_expression, anomaly_sensitivity, anomaly_direction, min_training_set_size, time_bucket, days_back, backfill_days, seasonality, mandatory_params=none, event_timestamp_column=none, freshness_column=none, sensitivity=none, ignore_small_changes={"spike_failure_percent_threshold": none, "drop_failure_percent_threshold": none}, fail_on_zero=false, detection_delay=none, anomaly_exclude_metrics=none, detection_period=none, training_period=none) %}
    {{ config(tags = ['elementary-tests']) }}
    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled()  %}
        {% set model_relation = elementary.get_model_relation_for_test(model, elementary.get_test_model()) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("The test has unsupported configuration, please contact Elementary support") }}
        {% endif %}

        {%- if elementary.is_ephemeral_model(model_relation) %}
            {{ exceptions.raise_compiler_error("The test is not supported for ephemeral models, model name: {}".format(model_relation.identifier)) }}
        {%- endif %}

        {% set test_table_name = elementary.get_elementary_test_table_name() %}
        {{ elementary.debug_log('collecting metrics for test: ' ~ test_table_name) }}
        {#- creates temp relation for test metrics -#}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}

        {#- get table configuration -#}
        {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}

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
                                                                                                   sensitivity=sensitivity,
                                                                                                   ignore_small_changes=ignore_small_changes,
                                                                                                   fail_on_zero=fail_on_zero,
                                                                                                   detection_delay=detection_delay,
                                                                                                   anomaly_exclude_metrics=anomaly_exclude_metrics,
                                                                                                   detection_period=detection_period,
                                                                                                   training_period=training_period) %}

        {% if not test_configuration %}
            {{ exceptions.raise_compiler_error("Failed to create test configuration dict for test `{}`".format(test_table_name)) }}
        {% endif %}
        {{ elementary.debug_log('test configuration - ' ~ test_configuration) }}
        {%- set table_monitors = elementary.get_final_table_monitors(table_anomalies) %}
        {{ elementary.debug_log('table_monitors - ' ~ table_monitors) }}
        {% if test_configuration.timestamp_column %}
            {%- set min_bucket_start, max_bucket_end = elementary.get_metric_buckets_min_and_max(model_relation=model_relation,
                                                                                            backfill_days=test_configuration.backfill_days,
                                                                                            days_back=test_configuration.days_back,
                                                                                            detection_delay=test_configuration.detection_delay,
                                                                                            metric_names=table_monitors,
                                                                                            metric_properties=metric_properties) %}
        {%- endif %}
        {{ elementary.debug_log('min_bucket_start: ' ~ min_bucket_start ~ ' | max_bucket_end: ' ~ max_bucket_end ) }}

        {#- execute table monitors and write to temp test table -#}
        {{ elementary.test_log('start', full_table_name) }}

        {% set table_metrics = [] %}
        {% for table_monitor in table_monitors %}
            {% do table_metrics.append({"type": table_monitor, "name": table_monitor}) %}
        {% endfor %}

        {%- set table_monitoring_query = elementary.table_monitoring_query(model,
                                                                           model_relation,
                                                                           min_bucket_start,
                                                                           max_bucket_end,
                                                                           table_metrics,
                                                                           metric_properties=metric_properties) %}
        {{ elementary.debug_log('table_monitoring_query - \n' ~ table_monitoring_query) }}
        {% set temp_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'metrics', table_monitoring_query) %}
        {#- calculate anomaly scores for metrics -#}
        {% set anomaly_scores_query = elementary.get_anomaly_scores_query(temp_table_relation,
                                                                          model_relation,
                                                                          test_configuration=test_configuration,
                                                                          metric_properties=metric_properties,
                                                                          metric_names=table_monitors) %}
        {{ elementary.debug_log('table monitors anomaly scores query - \n' ~ anomaly_scores_query) }}
        
        {% set anomaly_scores_test_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'anomaly_scores', anomaly_scores_query) %}
        {{ elementary.test_log('end', full_table_name) }}

        {% set flattened_test = elementary.flatten_test(elementary.get_test_model()) %}
        {% set anomaly_scores_sql = elementary.get_read_anomaly_scores_query() %}
        {% do elementary.store_metrics_table_in_cache() %}
        {% do elementary.store_anomaly_test_results(flattened_test, anomaly_scores_sql) %}

        {{ elementary.get_anomaly_query(flattened_test) }}
    {% else %}

        {# test must run an sql query #}
        {{ elementary.no_results_query() }}

    {% endif %}

{% endtest %}


