with metrics as (

    select * from {{ ref('data_monitoring_metrics') }}

),

 latest_metrics_times as (

     select
         full_table_name,
         column_name,
         metric_name,
         max(timeframe_end) as latest_timeframe_end,
         max(updated_at) as latest_updated_at
     from metrics
     group by 1,2,3

),

latest_metrics as (

     select
         m.id,
         m.full_table_name,
         m.column_name,
         m.metric_name,
         m.metric_value,
         m.timeframe_start,
         m.timeframe_end,
         m.timeframe_duration_hours,
         m.updated_at
     from metrics as m join latest_metrics_times as lm
        on (m.full_table_name = lm.full_table_name
            and m.column_name = lm.column_name
            and m.metric_name = lm.metric_name
            and (m.timeframe_end = lm.latest_timeframe_end or m.timeframe_end is null and m.updated_at = lm.latest_updated_at))
     where metric_value is not null

)

select * from latest_metrics