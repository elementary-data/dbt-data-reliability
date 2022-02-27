{{
  config(
    materialized='incremental',
    unique_key = 'id'
  )
}}

with monitors_run as (

    {{ monitors_query('1,2,3,4') }}

),

final_metrics as (

    select
        {{ dbt_utils.surrogate_key([
            'full_table_name',
            'column_name',
            'metric_name',
            'timeframe_start',
            'timeframe_end'
        ]) }} as id,
        full_table_name,
        column_name,
        metric_name,
        metric_value,
        timeframe_start,
        timeframe_end,
        timeframe_duration_hours,
        {{- dbt_utils.current_timestamp_in_utc() -}} as updated_at
    from monitors_run
)

select
    id,
    full_table_name,
    column_name,
    metric_name,
    metric_value,
    timeframe_start,
    timeframe_end,
    timeframe_duration_hours,
    max(updated_at) as updated_at
from final_metrics
group by 1,2,3,4,5,6,7,8