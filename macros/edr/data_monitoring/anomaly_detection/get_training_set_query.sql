{% macro get_training_set_query(full_table_name, column_name, metric_name, training_start, training_end, data_monitoring_metrics_relation) %}
    {% set training_set_query %}
        with data_monitoring_metrics as (
            select * from {{ data_monitoring_metrics_relation }}
            where bucket_end >= {{ elementary.cast_as_timestamp(elementary.const_as_string(training_start)) }}
                and bucket_end <= {{ elementary.cast_as_timestamp(elementary.const_as_string(training_end)) }}
                and upper(full_table_name) = upper({{ elementary.const_as_string(full_table_name) }})
                and metric_name = {{ elementary.const_as_string(metric_name) }}
                {%- if column_name %}
                    and upper(column_name) = upper({{ elementary.const_as_string(column_name) }})
                {%- endif %}
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
            from data_monitoring_metrics
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
                first_value(bucket_end) over (partition by metric_name, full_table_name, column_name order by edr_daily_bucket asc rows between {{ elementary.get_config_var('days_back') }} preceding and current row) as training_start,
                {{ elementary.get_config_var('anomaly_score_threshold') }} as anomaly_score_threshold,
                anomaly_score_threshold * training_stddev + training_avg as max_metric_value,
                (-1) * anomaly_score_threshold * training_stddev + training_avg as min_metric_value
            from daily_buckets left join
                grouped_metrics on (edr_daily_bucket = bucket_end)
            {{ dbt_utils.group_by(11) }}

        )

        select min_metric_value as min_value,
               max_metric_value as max_value,
               metric_value as value,
               bucket_start as start_time,
               bucket_end as end_time,
               metric_id
            from time_window_aggregation where min_metric_value is not NULL and max_metric_value is not NULL and metric_value is not NULL
    {% endset %}
    {{ return(training_set_query) }}
{% endmacro %}