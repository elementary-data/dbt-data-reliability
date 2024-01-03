{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with data_monitoring_metrics as (

    select * from {{ ref('data_monitoring_metrics') }}

),

time_window_aggregation as (

    select
        id,
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
        avg(metric_value) over (partition by metric_name, full_table_name, column_name order by bucket_start asc rows between unbounded preceding and current row) as training_avg,
        stddev(metric_value) over (partition by metric_name, full_table_name, column_name order by bucket_start asc rows between unbounded preceding and current row) as training_stddev,
        count(metric_value) over (partition by metric_name, full_table_name, column_name order by bucket_start asc rows between unbounded preceding and current row) as training_set_size,
        last_value(bucket_end) over (partition by metric_name, full_table_name, column_name order by bucket_start asc rows between unbounded preceding and current row) training_end,
        first_value(bucket_end) over (partition by metric_name, full_table_name, column_name order by bucket_start asc rows between unbounded preceding and current row) as training_start
    from data_monitoring_metrics
    {{ dbt_utils.group_by(12) }}
),

metrics_anomaly_score as (

    select
        id,
        full_table_name,
        column_name,
        dimension,
        dimension_value,
        metric_name,
        case
            when training_stddev is null then null
            when training_stddev = 0 then 0
            else (metric_value - training_avg) / (training_stddev)
        end as anomaly_score,
        metric_value as latest_metric_value,
        bucket_start,
        bucket_end,
        training_avg,
        training_stddev,
        training_start,
        training_end,
        training_set_size,
        max(updated_at) as updated_at
    from time_window_aggregation
        where
            metric_value is not null
            and training_avg is not null
            and bucket_end >= {{ elementary.edr_timeadd('day', '-7', elementary.edr_date_trunc('day', elementary.edr_current_timestamp())) }}
    {{ dbt_utils.group_by(15) }}
    order by bucket_end desc


),

final as (

    select
        id,
        full_table_name,
        column_name,
        dimension,
        dimension_value,
        metric_name,
        anomaly_score,
        latest_metric_value,
        bucket_start,
        bucket_end,
        training_avg,
        training_stddev,
        training_start,
        training_end,
        training_set_size,
        updated_at,
        case
            when abs(anomaly_score) > {{ elementary.get_config_var('anomaly_sensitivity') }} then true
            else false end
        as is_anomaly
    from metrics_anomaly_score
)

select * from final