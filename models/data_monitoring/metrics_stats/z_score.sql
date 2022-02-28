{{
  config(
    materialized='incremental',
    unique_key = 'id'
  )
}}

with stats as (

    select * from {{ ref('metrics_stats_for_anomalies') }}

),

latest_metrics as (

    select * from {{ ref('latest_metrics') }}

),

metrics_z_score as (

    select
        {{ dbt_utils.surrogate_key([
            'latest.full_table_name',
            'latest.column_name',
            'latest.metric_name',
            'stats.timeframe_end',
            'latest.id',
        ]) }} as id,
        latest.full_table_name,
        latest.column_name,
        latest.metric_name,
        case
            when stats.metric_stddev = 0 then 0
            else (latest.metric_value - stats.metric_avg) / (stats.metric_stddev)
        end as z_score,
        latest.metric_value as latest_value,
        latest.updated_at as value_updated_at,
        stats.metric_avg,
        stats.metric_stddev,
        stats.timeframe_start as stats_timeframe_start,
        stats.timeframe_end as stats_timeframe_end,
        stats.values_in_timeframe,
        {{ current_timestamp_column() }} as updated_at
    from latest_metrics as latest
        join stats
        on (stats.full_table_name = latest.full_table_name and
            stats.column_name = latest.column_name and
            stats.metric_name = latest.metric_name)
    where
        latest.metric_value is not null and
        stats.metric_avg is not null and
        stats.metric_stddev is not null and
        stats.values_in_timeframe > {{ var('minimal_sample_size') }}

),

final as (

    select
        id,
        full_table_name,
        column_name,
        metric_name,
        z_score,
        latest_value,
        value_updated_at,
        metric_avg,
        metric_stddev,
        stats_timeframe_start,
        stats_timeframe_end,
        values_in_timeframe,
        max(updated_at) as updated_at
    from metrics_z_score
    group by 1,2,3,4,5,6,7,8,9,10,11,12

)

select * from final



