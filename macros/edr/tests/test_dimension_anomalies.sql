{% test dimension_anomalies(model, dimensions, timestamp_column, where_expression, anomaly_sensitivity, anomaly_direction, min_training_set_size, time_bucket, days_back, backfill_days, seasonality, sensitivity,ignore_small_changes, fail_on_zero, detection_delay, anomaly_exclude_metrics, detection_period, training_period, exclude_final_results) %}
    {{ config(tags = ['elementary-tests']) }}
    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}
        {% set model_relation = elementary.get_model_relation_for_test(model, elementary.get_test_model()) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("Unsupported model: " ~ model ~ " (this might happen if you override 'ref' or 'source')") }}
        {% endif %}

        {%- if elementary.is_ephemeral_model(model_relation) %}
            {{ exceptions.raise_compiler_error("The test is not supported for ephemeral models, model name: {}".format(model_relation.identifier)) }}
        {%- endif %}
        {%- set mandatory_params = ['dimensions'] %}

        {% set test_table_name = elementary.get_elementary_test_table_name() %}
        {{ elementary.debug_log('collecting metrics for test: ' ~ test_table_name) }}
        {#- creates temp relation for test metrics -#}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}

        {#- get table configuration -#}
        {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}

        {%- set test_configuration, metric_properties = elementary.get_anomalies_test_configuration(model_relation=model_relation,
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
                                                                                                   dimensions=dimensions,
                                                                                                   sensitivity=sensitivity,
                                                                                                   ignore_small_changes=ignore_small_changes,
                                                                                                   fail_on_zero=fail_on_zero,
                                                                                                   detection_delay=detection_delay,
                                                                                                   anomaly_exclude_metrics=anomaly_exclude_metrics,
                                                                                                   detection_period=detection_period,
                                                                                                   training_period=training_period,
                                                                                                   exclude_final_results=exclude_final_results) %}

        {%- if not test_configuration %}
            {{ exceptions.raise_compiler_error("Failed to create test configuration dict for test `{}`".format(test_table_name)) }}
        {%- endif %}
        {{ elementary.debug_log('test configuration - ' ~ test_configuration) }}

        {%- set min_bucket_start, max_bucket_end = elementary.get_metric_buckets_min_and_max(model_relation=model_relation,
                                                                                backfill_days=test_configuration.backfill_days,
                                                                                days_back=test_configuration.days_back,
                                                                                detection_delay=test_configuration.detection_delay,
                                                                                metric_properties=metric_properties) %}

        {{ elementary.debug_log('min_bucket_start - ' ~ min_bucket_start) }}
        {#- execute table monitors and write to temp test table -#}
        {{ elementary.test_log('start', full_table_name) }}

        {%- set dimension_monitoring_query = elementary.dimension_monitoring_query(model, model_relation, metric_properties.dimensions, min_bucket_start, max_bucket_end, metric_properties) %}
        {{ elementary.debug_log('dimension_monitoring_query - \n' ~ dimension_monitoring_query) }}
        {% set temp_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'metrics', dimension_monitoring_query) %}
        {#- calculate anomaly scores for metrics -#}
        {% set anomaly_scores_query = elementary.get_anomaly_scores_query(test_metrics_table_relation=temp_table_relation,
                                                                          model_relation=model_relation,
                                                                          test_configuration=test_configuration,
                                                                          metric_names=['dimension'],
                                                                          metric_properties=metric_properties) %}

        {{ elementary.debug_log('dimension monitors anomaly scores query - \n' ~ anomaly_scores_query) }}
        {% set anomaly_scores_test_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'anomaly_scores', anomaly_scores_query) %}
        {{ elementary.test_log('end', full_table_name) }}

        {% set flattened_test = elementary.flatten_test(elementary.get_test_model()) %}
        {% set anomalous_dimension_rows_sql = elementary.get_anomaly_query_for_dimension_anomalies(flattened_test) %}

        {% do elementary.store_metrics_table_in_cache() %}
        {% do elementary.store_anomaly_test_results(flattened_test, anomalous_dimension_rows_sql) %}

        {{ elementary.get_anomaly_query(flattened_test) }}

    {% else %}

        {# test must run an sql query #}
        {{ elementary.no_results_query() }}

    {% endif %}

{% endtest %}
