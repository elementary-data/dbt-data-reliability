{% test column_anomalies(model, column_name, column_anomalies, timestamp_column, where_expression, anomaly_sensitivity, anomaly_direction, min_training_set_size, time_bucket, days_back, backfill_days, seasonality, sensitivity, detection_delay, anomaly_exclude_metrics) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('dbt_run_results') }}

    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {% set model_relation = elementary.get_model_relation_for_test(model, context["model"]) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("Unsupported model: " ~ model ~ " (this might happen if you override 'ref' or 'source')") }}
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

        {%- set test_configuration, metric_properties = elementary.get_anomalies_test_configuration(model_relation=model_relation,
                                                                                                   timestamp_column=timestamp_column,
                                                                                                   where_expression=where_expression,
                                                                                                   anomaly_sensitivity=anomaly_sensitivity,
                                                                                                   anomaly_direction=anomaly_direction,
                                                                                                   min_training_set_size=min_training_set_size,
                                                                                                   time_bucket=time_bucket,
                                                                                                   days_back=days_back,
                                                                                                   backfill_days=backfill_days,
                                                                                                   seasonality=seasonality,
                                                                                                   sensitivity=sensitivity,
                                                                                                   detection_delay=detection_delay,
                                                                                                   anomaly_exclude_metrics=anomaly_exclude_metrics) %}
        {%- if not test_configuration %}
            {{ exceptions.raise_compiler_error("Failed to create test configuration dict for test `{}`".format(test_table_name)) }}
        {%- endif %}
        {{ elementary.debug_log('test configuration - ' ~ test_configuration) }}

        {%- set column_obj_and_monitors = elementary.get_column_obj_and_monitors(model_relation, column_name, column_anomalies) -%}
        {%- if not column_obj_and_monitors -%}
            {{ exceptions.raise_compiler_error("Unable to find column `{}` in `{}`".format(column_name, full_table_name)) }}
        {%- endif -%}
        {%- set column_monitors = column_obj_and_monitors['monitors'] -%}
        {%- set column_obj = column_obj_and_monitors['column'] -%}
        {{ elementary.debug_log('column_monitors - ' ~ column_monitors) }}

        {% if test_configuration.timestamp_column %}
            {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=model_relation,
                                                                                    backfill_days=test_configuration.backfill_days,
                                                                                    days_back=test_configuration.days_back,
                                                                                    detection_delay=test_configuration.detection_delay,
                                                                                    monitors=column_monitors,
                                                                                    column_name=column_name,
                                                                                    metric_properties=metric_properties) %}
        {%- endif %}
        {{ elementary.debug_log('min_bucket_start - ' ~ min_bucket_start) }}
        {#- execute table monitors and write to temp test table -#}
        {{ elementary.test_log('start', full_table_name, column_name) }}
        {%- set column_monitoring_query = elementary.column_monitoring_query(model,
                                                                             model_relation,
                                                                             min_bucket_start,
                                                                             max_bucket_end,
                                                                             test_configuration.days_back,
                                                                             column_obj,
                                                                             column_monitors,
                                                                             metric_properties) %}
        {{ elementary.debug_log('column_monitoring_query - \n' ~ column_monitoring_query) }}
        {% set temp_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'metrics', column_monitoring_query) %}

        {#- calculate anomaly scores for metrics -#}
        {%- set temp_table_name = elementary.relation_to_full_name(temp_table_relation) %}
        {% set anomaly_scores_query = elementary.get_anomaly_scores_query(test_metrics_table_relation=temp_table_relation,
                                                                          model_relation=model_relation,
                                                                          test_configuration=test_configuration,
                                                                          monitors=column_monitors,
                                                                          column_name=column_name,
                                                                          metric_properties=metric_properties
                                                                          ) %}

        {{ elementary.debug_log('anomaly_score_query - \n' ~ anomaly_scores_query) }}
        {% set anomaly_scores_test_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'anomaly_scores', anomaly_scores_query) %}
        {{ elementary.test_log('end', full_table_name, column_name) }}

        {{ elementary.get_read_anomaly_scores_query() }}

    {%- else %}

        {#- test must run an sql query -#}
        {{ elementary.no_results_query() }}

    {%- endif %}
{% endtest %}



