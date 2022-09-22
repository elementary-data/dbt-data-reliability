{% macro get_anomaly_scores_query(test_metrics_table_relation, full_monitored_table_name, sensitivity, backfill_days, monitors, column_name = none, columns_only = false, dimensions = none) %}

    {%- set global_min_bucket_end = elementary.get_global_min_bucket_end_as_datetime() %}
    {%- set metrics_min_time = "'"~ (global_min_bucket_end - modules.datetime.timedelta(backfill_days)).strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set backfill_period = "'-" ~ backfill_days ~ "'" %}
    {%- set test_execution_id = elementary.get_test_execution_id() %}
    {%- set test_unique_id = elementary.get_test_unique_id() %}

    {% set anomaly_scores_query %}

        with data_monitoring_metrics as (

            select * from {{ ref('data_monitoring_metrics') }}
            {# We use bucket_end because non-timestamp tests have only bucket_end field. #}
            where bucket_end > {{ elementary.cast_as_timestamp(metrics_min_time) }}
                and upper(full_table_name) = upper('{{ full_monitored_table_name }}')
                and metric_name in {{ elementary.strings_list_to_tuple(monitors) }}
                {%- if column_name %}
                    and upper(column_name) = upper('{{ column_name }}')
                {%- endif %}
                {%- if columns_only %}
                    and column_name is not null
                {%- endif %}
                {% if dimensions %}
                    and dimension = {{ "'" ~ elementary.join_list(dimensions, '; ') ~ "'" }}
                {% endif %}

        ),

        union_metrics as (

            select * from data_monitoring_metrics
            union all
            select * from {{ test_metrics_table_relation }}

        ),

        grouped_metrics_duplicates as (

            select
                id,
                full_table_name,
                column_name,
                metric_name,
                metric_value,
                source_value,
                bucket_start,
                bucket_end,
                bucket_duration_hours,
                updated_at,
                dimension,
                dimension_value,
                row_number() over (partition by id order by updated_at desc) as row_number
            from union_metrics

        ),

        grouped_metrics as (

            select
                id as metric_id,
                full_table_name,
                column_name,
                dimension,
                dimension_value,
                metric_name,
                metric_value,
                source_value,
                bucket_start,
                bucket_end,
                bucket_duration_hours,
                updated_at
            from grouped_metrics_duplicates
            where row_number = 1

        ),

        daily_buckets as (

            {{ elementary.daily_buckets_cte() }}

        ),

        time_window_aggregation as (

            select
                metric_id,
                full_table_name,
                column_name,
                dimension,
                dimension_value,
                metric_name,
                metric_value,
                source_value,
                bucket_start,
                bucket_end,
                bucket_duration_hours,
                updated_at,
                edr_daily_bucket,
                avg(metric_value) over (partition by metric_name, full_table_name, column_name order by edr_daily_bucket asc rows between {{ elementary.get_config_var('days_back') }} preceding and current row) as training_avg,
                stddev(metric_value) over (partition by metric_name, full_table_name, column_name order by edr_daily_bucket asc rows between {{ elementary.get_config_var('days_back') }} preceding and current row) as training_stddev,
                count(metric_value) over (partition by metric_name, full_table_name, column_name order by edr_daily_bucket asc rows between {{ elementary.get_config_var('days_back') }} preceding and current row) as training_set_size,
                last_value(bucket_end) over (partition by metric_name, full_table_name, column_name order by edr_daily_bucket asc rows between {{ elementary.get_config_var('days_back') }} preceding and current row) training_end,
                first_value(bucket_end) over (partition by metric_name, full_table_name, column_name order by edr_daily_bucket asc rows between {{ elementary.get_config_var('days_back') }} preceding and current row) as training_start
            from daily_buckets left join
                grouped_metrics on (edr_daily_bucket = bucket_end)
            {{ dbt_utils.group_by(13) }}

        ),

        anomaly_scores as (

            select
                {{ dbt_utils.surrogate_key([
                 'metric_id',
                 elementary.const_as_string(test_execution_id)
                ]) }} as id,
                metric_id,
                {{ elementary.const_as_string(test_execution_id) }} as test_execution_id,
                {{ elementary.const_as_string(test_unique_id) }} as test_unique_id,
                {{ elementary.current_timestamp_column() }} as detected_at,
                full_table_name,
                column_name,
                metric_name,
                case
                    when training_stddev is null then null
                    when training_stddev = 0 then 0
                    else (metric_value - training_avg) / (training_stddev)
                end as anomaly_score,
                {{ sensitivity }} as anomaly_score_threshold,
                source_value as anomalous_value,
                bucket_start,
                bucket_end,
                metric_value,
                case 
                    when training_stddev is null then null
                    else (-1) * {{ sensitivity }} * training_stddev + training_avg
                end as min_metric_value,
                case 
                    when training_stddev is null then null
                    else {{ sensitivity }} * training_stddev + training_avg 
                end as max_metric_value,
                training_avg,
                training_stddev,
                training_set_size,
                training_start,
                training_end,
                dimension,
                dimension_value
            from time_window_aggregation
            where
                metric_value is not null
                and training_avg is not null
        )

        select * from anomaly_scores

    {% endset %}

    {{ return(anomaly_scores_query) }}
{% endmacro %}