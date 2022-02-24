
    select
        table_name,
        column_name,
        metric_name,
        avg(metric_value) as metric_avg,
        stddev(metric_value) as metric_stddev,
        min(timeframe_start) as timeframe_start,
        max(timeframe_end) as timeframe_end

    from
        {{ ref('table_metrics') }}
    where
        timeframe_start >= {{- zscore_timeframe_start() -}}
    group by
        table_name, column_name, metric_name
