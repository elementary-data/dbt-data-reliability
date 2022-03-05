with metrics as (

    select * from {{ ref('data_monitoring_metrics') }}

),

metrics_in_timeframe_for_aggregation as (

     select *
     from metrics
     where metric_value is not null
         and(timeframe_start is not null
         and timeframe_start >= {{- elementary.zscore_timeframe_start() -}})
         or updated_at >= {{- elementary.zscore_timeframe_start() -}}

),

metrics_aggregated_stats as (

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
    from metrics_in_timeframe_for_aggregation
    group by full_table_name, column_name, metric_name

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

),

final as (

    select
        lm.id,
        lm.full_table_name,
        lm.column_name,
        lm.metric_name,
        case
           when agg.metric_stddev = 0 then 0
           else (lm.metric_value - agg.metric_avg) / (agg.metric_stddev)
        end as z_score,
        lm.metric_value as latest_value,
        lm.updated_at as value_updated_at,
        agg.metric_avg,
        agg.metric_stddev,
        agg.timeframe_start as stats_timeframe_start,
        agg.timeframe_end as stats_timeframe_end,
        agg.values_in_timeframe,
        max(lm.updated_at) as updated_at
    from latest_metrics as lm
        join metrics_aggregated_stats as agg
        on (agg.full_table_name = lm.full_table_name and
            agg.column_name = lm.column_name and
            agg.metric_name = lm.metric_name)
        where
            lm.metric_value is not null
            and agg.metric_avg is not null
            and agg.metric_stddev is not null
            and agg.values_in_timeframe >= {{ var('days_back') - 1 }}
    group by 1,2,3,4,5,6,7,8,9,10,11,12

)

select * from final