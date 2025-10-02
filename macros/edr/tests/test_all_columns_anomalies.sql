{% test all_columns_anomalies(model, column_anomalies, exclude_prefix, exclude_regexp, timestamp_column, where_expression, anomaly_sensitivity, anomaly_direction, min_training_set_size, time_bucket, days_back, backfill_days, seasonality, sensitivity,ignore_small_changes, fail_on_zero, detection_delay, anomaly_exclude_metrics, detection_period, training_period, dimensions) %}
    {{ config(tags = ['elementary-tests']) }}
    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}
        {% set model_relation = elementary.get_model_relation_for_test(model, elementary.get_test_model()) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("Unsupported model: " ~ model ~ " (this might happen if you override 'ref' or 'source')") }}
        {% endif %}
        {%- if elementary.is_ephemeral_model(model_relation) %}
            {{ exceptions.raise_compiler_error("The test is not supported for ephemeral models, model name: {}".format(model_relation.identifier)) }}
        {%- endif %}

        {%- set test_table_name = elementary.get_elementary_test_table_name() %}
        {{- elementary.debug_log('collecting metrics for test: ' ~ test_table_name) }}
        {#- creates temp relation for test metrics -#}
        {%- set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}
        {%- set empty_table_query = elementary.empty_data_monitoring_metrics(with_created_at=false) %}
        {% set temp_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'metrics', empty_table_query) %}

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
                                                                                                   ignore_small_changes=ignore_small_changes,
                                                                                                   fail_on_zero=fail_on_zero,
                                                                                                   detection_delay=detection_delay,
                                                                                                   anomaly_exclude_metrics=anomaly_exclude_metrics,
                                                                                                   detection_period=detection_period,
                                                                                                   training_period=training_period,
                                                                                                   dimensions=dimensions) %}

        {%- if not test_configuration %}
            {{ exceptions.raise_compiler_error("Failed to create test configuration dict for test `{}`".format(test_table_name)) }}
        {%- endif %}
        {{ elementary.debug_log('test configuration - ' ~ test_configuration) }}

        {%- set column_objs_and_monitors = elementary.get_all_column_obj_and_monitors(model_relation, column_anomalies) -%}
        {#- execute table monitors and write to temp test table -#}
        {%- set monitors = [] %}
        {%- if column_objs_and_monitors | length > 0 %}
            {{- elementary.test_log('start', full_table_name, 'all columns') }}
            {%- for column_obj_and_monitors in column_objs_and_monitors %}
                {%- set column_obj = column_obj_and_monitors['column'] %}
                {%- set column_monitors = column_obj_and_monitors['monitors'] %}
                {%- set column_name = column_obj.name -%}
                {%- set ignore_column = elementary.should_ignore_column(column_name, exclude_regexp, exclude_prefix) -%}
                {%- if not ignore_column -%}
                    {%- do monitors.extend(column_monitors) -%}
                    {% if test_configuration.timestamp_column %}
                        {%- set min_bucket_start, max_bucket_end = elementary.get_metric_buckets_min_and_max(model_relation=model_relation,
                                                                                                backfill_days=test_configuration.backfill_days,
                                                                                                days_back=test_configuration.days_back,
                                                                                                detection_delay=test_configuration.detection_delay,
                                                                                                metric_names=column_monitors,
                                                                                                column_name=column_name,
                                                                                                metric_properties=metric_properties) %}
                    {%- endif %}

                    {{ elementary.debug_log('min_bucket_start - ' ~ min_bucket_start) }}
                    {{ elementary.test_log('start', full_table_name, column_name) }}

                    {% set metrics = [] %}
                    {% for monitor in column_monitors %}
                        {% do metrics.append({"name": monitor, "type": monitor}) %}
                    {% endfor %}

                    {%- set column_monitoring_query = elementary.column_monitoring_query(model, model_relation, min_bucket_start, max_bucket_end, test_configuration.days_back, column_obj, metrics, metric_properties, dimensions) %}
                    {%- do elementary.run_query(elementary.insert_as_select(temp_table_relation, column_monitoring_query)) -%}
                {%- else -%}
                    {{ elementary.debug_log('column ' ~ column_name ~ ' is excluded') }}
                {%- endif -%}
            {%- endfor %}
        {%- endif %}
        {%- set all_columns_monitors = monitors | unique | list %}
        {#- query if there is an anomaly in recent metrics -#}
        {%- set anomaly_scores_query = elementary.get_anomaly_scores_query(test_metrics_table_relation=temp_table_relation,
                                                                           model_relation=model_relation,
                                                                           test_configuration=test_configuration,
                                                                           metric_names=all_columns_monitors,
                                                                           columns_only=true,
                                                                           metric_properties=metric_properties) %}
        {% set anomaly_scores_test_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'anomaly_scores', anomaly_scores_query) %}

        {{- elementary.test_log('end', full_table_name, 'all columns') }}

        {% set flattened_test = elementary.flatten_test(elementary.get_test_model()) %}
        {% set anomaly_scores_sql = elementary.get_read_anomaly_scores_query() %}
        {% do elementary.store_metrics_table_in_cache() %}
        {% do elementary.store_anomaly_test_results(flattened_test, anomaly_scores_sql) %}

        {{ elementary.get_anomaly_query(flattened_test) }}

    {%- else %}

        {#- test must run an sql query -#}
        {{- elementary.no_results_query() }}

    {%- endif %}
{% endtest %}

{%- macro should_ignore_column(column_name, exclude_regexp, exclude_prefix) -%}
    {%- set regex_module = modules.re -%}
    {%- if exclude_regexp -%}
        {%- set is_match = regex_module.match(exclude_regexp, column_name, regex_module.IGNORECASE) %}
        {%- if is_match -%}
            {{ return(True) }}
        {%- endif -%}
    {%- endif -%}
    {% if exclude_prefix %}
        {%- set exclude_regexp = '^' ~ exclude_prefix ~ '.*' %}
        {%- set is_match = regex_module.match(exclude_regexp, column_name, regex_module.IGNORECASE) %}
        {%- if is_match -%}
            {{ return(True) }}
        {%- endif -%}
    {%- endif -%}
    {{ return(False) }}
{%- endmacro -%}

