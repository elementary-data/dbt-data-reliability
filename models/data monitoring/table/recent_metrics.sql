    select
        table_name,
        column_name,
        metric_name,
        metric_value as recent_value,
        timeframe_start,
        timeframe_end
    from
        {{ ref('table_metrics') }}
    where
        timeframe_end = {{ max_timeframe_end() }}