{% test column_value_anomalies(
    model,
    column_name,
    timestamp_column,
    where_expression,
    anomaly_sensitivity,
    anomaly_direction,
    min_training_set_size,
    days_back,
    backfill_days,
    seasonality,
    sensitivity,
    fail_on_zero,
    detection_delay,
    detection_period,
    training_period,
    exclude_detection_period_from_training=false
) %}
    {{ config(tags=["elementary-tests"]) }}
    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}
        {% set model_relation = elementary.get_model_relation_for_test(
            model, elementary.get_test_model()
        ) %}
        {% if not model_relation %}
            {{
                exceptions.raise_compiler_error(
                    "Unsupported model: "
                    ~ model
                    ~ " (this might happen if you override 'ref' or 'source')"
                )
            }}
        {% endif %}

        {%- if elementary.is_ephemeral_model(model_relation) %}
            {{
                exceptions.raise_compiler_error(
                    "The test is not supported for ephemeral models, model name: {}".format(
                        model_relation.identifier
                    )
                )
            }}
        {%- endif %}

        {% set test_table_name = elementary.get_elementary_test_table_name() %}
        {{ elementary.debug_log("collecting metrics for test: " ~ test_table_name) }}
        {#- creates temp relation for test metrics -#}
        {% set database_name, schema_name = (
            elementary.get_package_database_and_schema("elementary")
        ) %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(
            database_name, schema_name
        ) %}

        {#- get table configuration -#}
        {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}

        {#- For column_value_anomalies we need a time_bucket for the configuration infrastructure,
            but we use it only internally for period calculations. We default to day/1 since
            the test operates on raw values, not bucketed aggregates. -#}
        {%- set default_time_bucket = {"period": "day", "count": 1} %}

        {%- set test_configuration, metric_properties = (
            elementary.get_anomalies_test_configuration(
                model_relation=model_relation,
                mandatory_params=none,
                timestamp_column=timestamp_column,
                where_expression=where_expression,
                anomaly_sensitivity=anomaly_sensitivity,
                anomaly_direction=anomaly_direction,
                min_training_set_size=min_training_set_size,
                time_bucket=default_time_bucket,
                days_back=days_back,
                backfill_days=backfill_days,
                seasonality=seasonality,
                freshness_column=none,
                event_timestamp_column=none,
                dimensions=none,
                sensitivity=sensitivity,
                ignore_small_changes=none,
                fail_on_zero=fail_on_zero,
                detection_delay=detection_delay,
                anomaly_exclude_metrics=none,
                detection_period=detection_period,
                training_period=training_period,
                exclude_final_results=none,
                exclude_detection_period_from_training=exclude_detection_period_from_training,
            )
        ) %}

        {%- if not test_configuration %}
            {{
                exceptions.raise_compiler_error(
                    "Failed to create test configuration dict for test `{}`".format(
                        test_table_name
                    )
                )
            }}
        {%- endif %}
        {{ elementary.debug_log("test configuration - " ~ test_configuration) }}

        {#- Validate that timestamp_column is set (required for this test) -#}
        {%- if not test_configuration.timestamp_column %}
            {{
                exceptions.raise_compiler_error(
                    "column_value_anomalies requires a timestamp_column to split data into training and detection periods."
                )
            }}
        {%- endif %}

        {#- Get column object to validate the column exists -#}
        {%- set columns = adapter.get_columns_in_relation(model_relation) %}
        {%- set column_obj = none %}
        {%- for col in columns %}
            {%- if col.name | lower == column_name | lower %}
                {%- set column_obj = col %}
            {%- endif %}
        {%- endfor %}
        {%- if not column_obj %}
            {{
                exceptions.raise_compiler_error(
                    "Unable to find column `{}` in `{}`".format(
                        column_name, full_table_name
                    )
                )
            }}
        {%- endif %}

        {#- Calculate detection end and training start -#}
        {%- set detection_end = elementary.get_detection_end(
            test_configuration.detection_delay
        ) %}
        {%- set detection_end_expr = elementary.edr_cast_as_timestamp(
            elementary.edr_datetime_to_sql(detection_end)
        ) %}
        {%- set training_start = (
            detection_end - modules.datetime.timedelta(days=test_configuration.days_back)
        ) %}
        {%- set training_start_expr = elementary.edr_cast_as_timestamp(
            elementary.edr_datetime_to_sql(training_start)
        ) %}
        {%- set detection_start = (
            detection_end - modules.datetime.timedelta(days=test_configuration.backfill_days)
        ) %}
        {%- set detection_start_expr = elementary.edr_cast_as_timestamp(
            elementary.edr_datetime_to_sql(detection_start)
        ) %}

        {#- Build seasonality expression -#}
        {%- set ts_col = test_configuration.timestamp_column %}
        {%- if test_configuration.seasonality == "day_of_week" %}
            {%- set seasonality_expr = elementary.edr_day_of_week_expression(ts_col) %}
            {%- set has_seasonality = true %}
        {%- elif test_configuration.seasonality == "hour_of_day" %}
            {%- set seasonality_expr = elementary.edr_hour_of_day_expression(ts_col) %}
            {%- set has_seasonality = true %}
        {%- elif test_configuration.seasonality == "hour_of_week" %}
            {%- set seasonality_expr = elementary.edr_hour_of_week_expression(ts_col) %}
            {%- set has_seasonality = true %}
        {%- else %}
            {%- set seasonality_expr = elementary.const_as_text("no_seasonality") %}
            {%- set has_seasonality = false %}
        {%- endif %}

        {#- Build the column value anomalies query -#}
        {{ elementary.test_log("start", full_table_name, column_name) }}

        {%- set quoted_column = adapter.quote(column_name) %}

        {#- Step 1: Build a metrics-like table with individual row values as metrics.
            This allows us to feed into the existing anomaly_scores infrastructure. -#}
        {%- set column_value_metrics_query %}
            with monitored_table as (
                select *
                from {{ model }}
                {% if metric_properties.where_expression %}
                    where {{ metric_properties.where_expression }}
                {% endif %}
            ),

            row_values as (
                select
                    {{ elementary.edr_cast_as_string(elementary.edr_quote(elementary.relation_to_full_name(model_relation))) }} as full_table_name,
                    {{ elementary.edr_cast_as_string(elementary.edr_quote(column_name)) }} as column_name,
                    {{ elementary.edr_cast_as_string("'column_value'") }} as metric_name,
                    {{ elementary.edr_cast_as_string("'column_value'") }} as metric_type,
                    {{ elementary.edr_cast_as_float(quoted_column) }} as metric_value,
                    {{ elementary.edr_cast_as_string(quoted_column) }} as source_value,
                    {{ elementary.edr_cast_as_timestamp(ts_col) }} as row_timestamp,
                    {{ seasonality_expr }} as bucket_seasonality
                from monitored_table
                where {{ elementary.edr_cast_as_timestamp(ts_col) }} >= {{ training_start_expr }}
                  and {{ elementary.edr_cast_as_timestamp(ts_col) }} < {{ detection_end_expr }}
                  and {{ quoted_column }} is not null
            ),

            training_data as (
                select *
                from row_values
                where row_timestamp < {{ detection_start_expr }}
            ),

            detection_data as (
                select *
                from row_values
                where row_timestamp >= {{ detection_start_expr }}
            ),

            {#- Compute baseline statistics from training period -#}
            training_stats as (
                select
                    {% if has_seasonality %}
                        bucket_seasonality,
                    {% endif %}
                    avg(metric_value) as training_avg,
                    {{ elementary.standard_deviation("metric_value") }} as training_stddev,
                    count(metric_value) as training_set_size,
                    min(row_timestamp) as training_start,
                    max(row_timestamp) as training_end
                from training_data
                {% if has_seasonality %}
                    group by bucket_seasonality
                {% endif %}
            ),

            {#- Join detection data with training stats and compute z-scores -#}
            anomaly_scores as (
                select
                    {{ elementary.generate_surrogate_key([
                        "d.full_table_name",
                        "d.column_name",
                        "d.metric_name",
                        "d.source_value",
                        elementary.edr_cast_as_string("d.row_timestamp")
                    ]) }} as id,
                    {{ elementary.generate_surrogate_key([
                        "d.full_table_name",
                        "d.column_name",
                        "d.metric_name",
                        "d.source_value",
                        elementary.edr_cast_as_string("d.row_timestamp")
                    ]) }} as metric_id,
                    {{ elementary.const_as_string(elementary.get_test_execution_id()) }} as test_execution_id,
                    {{ elementary.const_as_string(elementary.get_test_unique_id()) }} as test_unique_id,
                    {{ elementary.current_timestamp_column() }} as detected_at,
                    d.full_table_name,
                    d.column_name,
                    d.metric_name,
                    case
                        when {{ elementary.edr_normalize_stddev('s.training_stddev') }} is null then null
                        when s.training_set_size < 2 then null
                        when {{ elementary.edr_normalize_stddev('s.training_stddev') }} = 0 then
                            case when d.metric_value = s.training_avg then 0
                            else null end
                        else (d.metric_value - s.training_avg) / {{ elementary.edr_normalize_stddev('s.training_stddev') }}
                    end as anomaly_score,
                    {{ test_configuration.anomaly_sensitivity }} as anomaly_score_threshold,
                    d.source_value as anomalous_value,
                    {{ elementary.edr_cast_as_timestamp("d.row_timestamp") }} as bucket_start,
                    {{ elementary.edr_cast_as_timestamp("d.row_timestamp") }} as bucket_end,
                    d.bucket_seasonality,
                    d.metric_value,
                    case
                        when {{ elementary.edr_normalize_stddev('s.training_stddev') }} is null or s.training_set_size < 2 then null
                        else ((-1) * {{ test_configuration.anomaly_sensitivity }} * {{ elementary.edr_normalize_stddev('s.training_stddev') }} + s.training_avg)
                    end as min_metric_value,
                    case
                        when {{ elementary.edr_normalize_stddev('s.training_stddev') }} is null or s.training_set_size < 2 then null
                        else ({{ test_configuration.anomaly_sensitivity }} * {{ elementary.edr_normalize_stddev('s.training_stddev') }} + s.training_avg)
                    end as max_metric_value,
                    s.training_avg,
                    {{ elementary.edr_normalize_stddev('s.training_stddev') }} as training_stddev,
                    s.training_set_size,
                    {{ elementary.edr_cast_as_timestamp("s.training_start") }} as training_start,
                    {{ elementary.edr_cast_as_timestamp("s.training_end") }} as training_end,
                    {{ elementary.null_string() }} as dimension,
                    {{ elementary.null_string() }} as dimension_value
                from detection_data d
                left join training_stats s
                {% if has_seasonality %}
                    on d.bucket_seasonality = s.bucket_seasonality
                {% else %}
                    on 1 = 1
                {% endif %}
            )

            select * from anomaly_scores
        {%- endset %}

        {#- Create the anomaly scores test table -#}
        {% set anomaly_scores_test_table_relation = (
            elementary.create_elementary_test_table(
                database_name,
                tests_schema_name,
                test_table_name,
                "anomaly_scores",
                column_value_metrics_query,
            )
        ) %}
        {{ elementary.test_log("end", full_table_name, column_name) }}

        {#- Store results using the existing infrastructure -#}
        {% set flattened_test = elementary.flatten_test(elementary.get_test_model()) %}
        {% set anomaly_scores_sql = elementary.get_read_anomaly_scores_query() %}
        {% do elementary.store_anomaly_test_results(
            flattened_test, anomaly_scores_sql
        ) %}

        {{ elementary.get_anomaly_query(flattened_test) }}

    {%- else %}

        {#- test must run an sql query -#}
        {{ elementary.no_results_query() }}

    {%- endif %}
{% endtest %}
