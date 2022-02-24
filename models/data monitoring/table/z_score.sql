    select
        stats.table_name,
        stats.column_name,
        stats.metric_name,
        (recent.recent_value - stats.metric_avg) / (stats.metric_stddev) as z_score,
        recent.recent_value,
        stats.metric_avg,
        stats.metric_stddev,
        recent.timeframe_end
    from
        {{ ref('metrics_stats') }} as stats,
        {{ ref('recent_metrics') }} as recent
    where
        stats.table_name = recent.table_name and
        stats.column_name = recent.column_name and
        stats.metric_name = recent.metric_name and
        recent.recent_value is not null and
        stats.metric_avg is not null and
        stats.metric_stddev is not null
