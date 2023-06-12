{% test all_columns_anomalies(model, column_anomalies, exclude_prefix, exclude_regexp, timestamp_column, where_expression, anomaly_sensitivity, anomaly_direction, min_training_set_size, time_bucket, days_back, backfill_days, seasonality, sensitivity) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('alerts_anomaly_detection') }}
    -- depends_on: {{ ref('metrics_anomaly_score') }}
    -- depends_on: {{ ref('dbt_run_results') }}
    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {%- if elementary.is_ephemeral_model(model) %}
            {{ exceptions.raise_compiler_error("The test is not supported for ephemeral models, model name: {}".format(model.identifier)) }}
        {%- endif %}

        {%- set test_table_name = elementary.get_elementary_test_table_name() %}
        {{- elementary.debug_log('collecting metrics for test: ' ~ test_table_name) }}
        {#- creates temp relation for test metrics -#}
        {%- set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}
        {%- set empty_table_query = elementary.empty_data_monitoring_metrics(with_created_at=false) %}
        {% set temp_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'metrics', empty_table_query) %}

        {#- get table configuration -#}
        {%- set full_table_name = elementary.relation_to_full_name(model) %}
        {%- set model_relation = dbt.load_relation(model) %}
        {% if not model_relation %}
            {%- set model_relation = model %}
            {%- do elementary.edr_log("Unable to load_relation for table `{}`".format(full_table_name) -%}
        {% endif %}

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
                                                                                                   sensitivity=sensitivity) %}
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
                        {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=model,
                                                                                                backfill_days=test_configuration.backfill_days,
                                                                                                days_back=test_configuration.days_back,
                                                                                                monitors=column_monitors,
                                                                                                column_name=column_name,
                                                                                                metric_properties=metric_properties) %}
                    {%- endif %}
                    {{ elementary.debug_log('min_bucket_start - ' ~ min_bucket_start) }}
                    {{ elementary.test_log('start', full_table_name, column_name) }}
                    {%- set column_monitoring_query = elementary.column_monitoring_query(model_relation, min_bucket_start, max_bucket_end, test_configuration.days_back, column_obj, column_monitors, metric_properties) %}
                    {%- do run_query(elementary.insert_as_select(temp_table_relation, column_monitoring_query)) -%}
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
                                                                           monitors=all_columns_monitors,
                                                                           columns_only=true,
                                                                           metric_properties=metric_properties) %}

        {% set anomaly_scores_test_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'anomaly_scores', anomaly_scores_query) %}

        {{- elementary.test_log('end', full_table_name, 'all columns') }}

        {{ elementary.get_read_anomaly_scores_query() }}

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

