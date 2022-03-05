with metrics as (

    select * from {{ ref('data_monitoring_metrics') }}

),

metrics_in_timeframe_for_training as (

     select *
     from metrics
     where metric_value is not null
         and(timeframe_start is not null
         and timeframe_start >= {{- elementary.zscore_timeframe_start() -}})
         or updated_at >= {{- elementary.zscore_timeframe_start() -}}

),

metrics_training_set as (

    select
        full_table_name,
        column_name,
        metric_name,
        avg(metric_value) as metric_avg,
        stddev(metric_value) as metric_stddev,
        count(id) as values_in_timeframe,
        case
            when min(timeframe_start) is not null then min(timeframe_start)
            else min(updated_at)
        end as timeframe_start,
        case
            when max(timeframe_start) is not null then max(timeframe_start)
            else max(updated_at)
        end as timeframe_end
    from metrics_in_timeframe_for_training
    group by full_table_name, column_name, metric_name

),

metrics_times_for_validation as (

     select
         full_table_name,
         column_name,
         metric_name,
         max(timeframe_end) as latest_timeframe_end,
         max(updated_at) as latest_updated_at
     from metrics
     group by 1,2,3

),

metrics_validation_set as (

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
     from metrics as m join metrics_times_for_validation as tv
        on (m.full_table_name = tv.full_table_name
            and m.column_name = tv.column_name
            and m.metric_name = tv.metric_name
            and (m.timeframe_end = tv.latest_timeframe_end or m.timeframe_end is null and m.updated_at = tv.latest_updated_at))
     where metric_value is not null

),

final as (

    select
        vl.id,
        vl.full_table_name,
        vl.column_name,
        vl.metric_name,
        case
           when tr.metric_stddev = 0 then 0
           else (vl.metric_value - tr.metric_avg) / (tr.metric_stddev)
        end as z_score,
        vl.metric_value as latest_value,
        vl.updated_at as value_updated_at,
        tr.metric_avg,
        tr.metric_stddev,
        tr.timeframe_start as training_timeframe_start,
        tr.timeframe_end as training_timeframe_end,
        tr.values_in_timeframe,
        max(vl.updated_at) as updated_at
    from metrics_validation_set as vl
        join metrics_training_set  as tr
        on (tr.full_table_name = vl.full_table_name and
            tr.column_name = vl.column_name and
            tr.metric_name = vl.metric_name)
        where
            vl.metric_value is not null
            and tr.metric_avg is not null
            and tr.metric_stddev is not null
            and tr.values_in_timeframe >= {{ var('days_back') - 1 }}
    group by 1,2,3,4,5,6,7,8,9,10,11,12

)

select * from final