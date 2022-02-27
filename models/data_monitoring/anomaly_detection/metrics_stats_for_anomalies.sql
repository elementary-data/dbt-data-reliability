with metrics as (

    select * from {{ ref('data_monitoring_metrics') }}

),

metrics_timestamp_based as (

    select *
    from metrics
    where timeframe_start is not null
        and timeframe_start >= {{- zscore_timeframe_start() -}}

),

metrics_timestamp_based_stats as (

    select
        full_table_name,
        column_name,
        metric_name,
        avg(metric_value) as metric_avg,
        stddev(metric_value) as metric_stddev,
        count(id) as values_in_timeframe,
        min(timeframe_start) as timeframe_start,
        max(timeframe_end) as timeframe_end,
        {{ dbt_utils.datediff('min(timeframe_start)', 'max(timeframe_end)', 'hour') }} as timeframe_duration_hours
    from metrics_timestamp_based
    group by full_table_name, column_name, metric_name

),

metrics_no_timestamp as (

    select *
    from metrics
    where timeframe_start is null
    and updated_at >= {{- zscore_timeframe_start() -}}

),

metrics_no_timestamp_stats as (

    select
        full_table_name,
        column_name,
        metric_name,
        avg(metric_value) as metric_avg,
        stddev(metric_value) as metric_stddev,
        count(id) as values_in_timeframe,
        min(updated_at) as timeframe_start,
        max(updated_at) as timeframe_end,
        {{ dbt_utils.datediff('min(updated_at)', 'max(updated_at)', 'hour') }} as timeframe_duration_hours
    from metrics_no_timestamp
    group by full_table_name, column_name, metric_name

),

metrics_stats as (

    select * from metrics_timestamp_based_stats
    union all
    select * from metrics_no_timestamp_stats
)

select * from metrics_stats