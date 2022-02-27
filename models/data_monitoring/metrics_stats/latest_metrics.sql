with metrics as (

    select * from {{ ref('data_monitoring_metrics') }}

),

latest_metrics_times as (

    select
        full_table_name,
        column_name,
        metric_name,
        max(timeframe_end) over (partition by full_table_name, column_name, metric_name) as latest_timeframe_end
    from metrics
    where timeframe_end is not null

),

latest_metrics_no_times as (

    select
        full_table_name,
        column_name,
        metric_name,
        max(updated_at) over (partition by full_table_name, column_name, metric_name) as latest_updated_at
    from metrics
    where timeframe_end is null

),

latest_time_based_metrics as (

    select
        m.id,
        m.full_table_name,
        m.column_name,
        m.metric_name,
        m.metric_value,
        m.timeframe_start,
        m.timeframe_end,
        m.timeframe_duration_hours,
        updated_at
    from metrics as m join latest_metrics_times as lm
        on (m.full_table_name = lm.full_table_name
            and m.column_name = lm.column_name
            and m.metric_name = lm.metric_name
            and m.timeframe_end = lm.latest_timeframe_end)
),

latest_no_time_metrics as (

     select
         m.id,
         m.full_table_name,
         m.column_name,
         m.metric_name,
         m.metric_value,
         m.timeframe_start,
         m.timeframe_end,
         m.timeframe_duration_hours,
         updated_at
     from metrics as m join latest_metrics_no_times as lm
        on (m.full_table_name = lm.full_table_name
            and m.column_name = lm.column_name
            and m.metric_name = lm.metric_name
            and m.updated_at = lm.latest_updated_at)
),

latest_metrics as (

    select * from latest_time_based_metrics
    union all
    select * from latest_no_time_metrics

)

select * from latest_metrics