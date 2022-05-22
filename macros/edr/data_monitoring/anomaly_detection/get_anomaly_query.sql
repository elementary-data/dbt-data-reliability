{% macro get_anomaly_query(test_metrics_table_relation, full_monitored_table_name, monitors, timestamp_column, column_name = none, columns_only=false) %}

    {%- set global_min_bucket_start = elementary.get_global_min_bucket_start_as_datetime() %}
    {%- set metrics_min_time = "'"~ (global_min_bucket_start - modules.datetime.timedelta(elementary.get_config_var('backfill_days_per_run'))).strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set backfill_period = "'-" ~ elementary.get_config_var('backfill_days_per_run') ~ "'" %}
    {%- set test_execution_id = elementary.get_test_execution_id() %}
    {%- set test_unique_id = elementary.get_test_unique_id() %}

    {% set anomaly_query %}

        with data_monitoring_metrics as (

            select * from {{ ref('data_monitoring_metrics') }}
            where bucket_start > {{ elementary.cast_as_timestamp(metrics_min_time) }}
                and upper(full_table_name) = upper('{{ full_monitored_table_name }}')
                and metric_name in {{ elementary.strings_list_to_tuple(monitors) }}
                {%- if column_name %}
                    and upper(column_name) = upper('{{ column_name }}')
                {%- endif %}
                {%- if columns_only %}
                    and column_name is not null
                {%- endif %}

        ),


        union_metrics as (

            select * from data_monitoring_metrics
            union all
            select * from {{ test_metrics_table_relation }}

        ),

        grouped_metrics_duplicates as (

            select
                *,
                row_number() over (partition by id order by updated_at desc) as row_number
            from union_metrics

        ),

        grouped_metrics as (

            select
                id as metric_id,
                full_table_name,
                column_name,
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
                *,
                avg(metric_value) over (partition by metric_name, full_table_name, column_name order by edr_daily_bucket asc rows between {{ elementary.get_config_var('days_back') }} preceding and current row) as training_avg,
                stddev(metric_value) over (partition by metric_name, full_table_name, column_name order by edr_daily_bucket asc rows between {{ elementary.get_config_var('days_back') }} preceding and current row) as training_stddev,
                count(metric_value) over (partition by metric_name, full_table_name, column_name order by edr_daily_bucket asc rows between {{ elementary.get_config_var('days_back') }} preceding and current row) as training_set_size,
                last_value(bucket_end) over (partition by metric_name, full_table_name, column_name order by edr_daily_bucket asc rows between {{ elementary.get_config_var('days_back') }} preceding and current row) training_end,
                first_value(bucket_end) over (partition by metric_name, full_table_name, column_name order by edr_daily_bucket asc rows between {{ elementary.get_config_var('days_back') }} preceding and current row) as training_start
            from daily_buckets left join
                grouped_metrics on (edr_daily_bucket = bucket_end)
            {{ dbt_utils.group_by(11) }}

        ),

        calc_anomaly_score as (

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
                    when training_stddev = 0 then 0
                    else (metric_value - training_avg) / (training_stddev)
                end as anomaly_score,
                {{ elementary.get_config_var('anomaly_score_threshold') }} as anomaly_score_threshold,
                {{ elementary.anomaly_detection_description() }},
                source_value as anomalous_value,
                bucket_start,
                bucket_end,
                metric_value as latest_metric_value,
                training_avg,
                training_stddev,
                training_set_size,
                {{ elementary.const_as_string(timestamp_column) }} as timestamp_column
            from time_window_aggregation
            where
                metric_value is not null
              and training_avg is not null
              and training_stddev is not null
            {# training dataset minimal size to make anomaly detection relevant #}
              and training_set_size >= {{ elementary.get_config_var('days_back') -1 }}
            {# get anomalies for the whole backfill timeframe #}
              and bucket_end >= {{ elementary.timeadd('day', backfill_period, elementary.get_max_bucket_end()) }}

        )

        select * from calc_anomaly_score
        where abs(anomaly_score) > {{ elementary.get_config_var('anomaly_score_threshold') }}

    {% endset %}

    {{ return(anomaly_query) }}
{% endmacro %}